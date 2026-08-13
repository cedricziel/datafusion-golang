package iceberg_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	ib "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/cedricziel/datafusion-golang/datafusion"
	provider "github.com/cedricziel/datafusion-golang/providers/iceberg"
)

// fullScanOnly hides ScanWithOptions so the engine registers the provider
// without pushdown — the reference side of the differential tests.
type fullScanOnly struct {
	p datafusion.TableProvider
}

func (f fullScanOnly) Schema() *arrow.Schema { return f.p.Schema() }
func (f fullScanOnly) Scan(ctx context.Context) (array.RecordReader, error) {
	return f.p.Scan(ctx)
}

func pushdownSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "part", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	}, nil)
}

// newPartitionedFixture builds a real Iceberg table identity-partitioned
// on "part" with several appends, so the current snapshot spans multiple
// data files across multiple partitions. Every third name is null.
func newPartitionedFixture(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	sc := pushdownSchema()

	warehouse := t.TempDir()
	cat, err := hadoop.NewCatalog("test", warehouse, nil)
	if err != nil {
		t.Fatalf("hadoop.NewCatalog: %v", err)
	}
	if err := cat.CreateNamespace(ctx, []string{"default"}, nil); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	icebergSchema, err := icebergtable.ArrowSchemaToIcebergWithFreshIDs(sc, false)
	if err != nil {
		t.Fatalf("ArrowSchemaToIcebergWithFreshIDs: %v", err)
	}
	partField, ok := icebergSchema.FindFieldByName("part")
	if !ok {
		t.Fatal("field part not found in converted schema")
	}
	spec := ib.NewPartitionSpec(ib.PartitionField{
		SourceIDs: []int{partField.ID},
		FieldID:   1000,
		Name:      "part",
		Transform: ib.IdentityTransform{},
	})
	tbl, err := cat.CreateTable(ctx, []string{"default", "events"}, icebergSchema, catalog.WithPartitionSpec(&spec))
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Two appends of 30 rows each over partitions 1..3, ids ascending.
	for a := 0; a < 2; a++ {
		b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
		for i := 0; i < 30; i++ {
			id := int64(a*30 + i + 1)
			b.Field(0).(*array.Int64Builder).Append(id%3 + 1)
			b.Field(1).(*array.Int64Builder).Append(id)
			if id%3 == 0 {
				b.Field(2).(*array.StringBuilder).AppendNull()
			} else {
				b.Field(2).(*array.StringBuilder).Append(fmt.Sprintf("n-%d", id))
			}
			b.Field(3).(*array.TimestampBuilder).Append(arrow.Timestamp(1_700_000_000_000_000 + id*1_000_000))
		}
		rec := b.NewRecordBatch()
		rdr, err := array.NewRecordReader(sc, []arrow.RecordBatch{rec})
		if err != nil {
			t.Fatalf("NewRecordReader: %v", err)
		}
		next, err := tbl.Append(ctx, rdr, nil)
		rdr.Release()
		rec.Release()
		b.Release()
		if err != nil {
			t.Fatalf("Append %d: %v", a, err)
		}
		tbl = next
	}
	return tbl.MetadataLocation()
}

func collectAll(t *testing.T, reader array.RecordReader) []string {
	t.Helper()
	defer reader.Release()
	var rows []string
	for reader.Next() {
		rec := reader.RecordBatch()
		for i := 0; i < int(rec.NumRows()); i++ {
			row := ""
			for c := 0; c < int(rec.NumCols()); c++ {
				col := rec.Column(c)
				if col.IsNull(i) {
					row += "|NULL"
				} else {
					row += "|" + col.ValueStr(i)
				}
			}
			rows = append(rows, row)
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reading result: %v", err)
	}
	return rows
}

func querySorted(t *testing.T, ctx *datafusion.SessionContext, q string) []string {
	t.Helper()
	reader, err := ctx.SQL(q)
	if err != nil {
		t.Fatalf("SQL %q: %v", q, err)
	}
	rows := collectAll(t, reader)
	sort.Strings(rows)
	return rows
}

func newDiffSessions(t *testing.T, metaLoc string) (push, full *datafusion.SessionContext) {
	t.Helper()
	p, err := provider.NewTableProvider(context.Background(), metaLoc)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	if _, ok := p.(datafusion.PushdownTableProvider); !ok {
		t.Fatal("iceberg provider must implement PushdownTableProvider")
	}

	push, err = datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	t.Cleanup(func() { push.Close() })
	if err := push.RegisterTable("t", p); err != nil {
		t.Fatalf("RegisterTable pushdown: %v", err)
	}

	full, err = datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	t.Cleanup(func() { full.Close() })
	if err := full.RegisterTable("t", fullScanOnly{p}); err != nil {
		t.Fatalf("RegisterTable full-scan: %v", err)
	}
	return push, full
}

func TestPushdown_DifferentialBattery(t *testing.T) {
	metaLoc := newPartitionedFixture(t)
	push, full := newDiffSessions(t, metaLoc)

	queries := []string{
		"SELECT * FROM t",
		// Partition pruning.
		"SELECT * FROM t WHERE part = 2",
		"SELECT * FROM t WHERE part IN (1, 3)",
		"SELECT * FROM t WHERE part = 99",
		// Column-statistics pruning on the sorted id column.
		"SELECT * FROM t WHERE id > 45",
		"SELECT * FROM t WHERE id <= 5",
		"SELECT * FROM t WHERE id = 33",
		"SELECT * FROM t WHERE id = 4000",
		"SELECT * FROM t WHERE id <> 7",
		"SELECT * FROM t WHERE id BETWEEN 28 AND 33",
		"SELECT * FROM t WHERE id NOT BETWEEN 5 AND 55",
		"SELECT * FROM t WHERE id IN (3, 17, 300)",
		// NOT IN over a column with NULLs present (name is null every 3rd row).
		"SELECT * FROM t WHERE name NOT IN ('n-1', 'n-2')",
		"SELECT * FROM t WHERE name IN ('n-1', 'n-44')",
		"SELECT * FROM t WHERE name IS NULL",
		"SELECT * FROM t WHERE name IS NOT NULL",
		// NOT / De Morgan shapes.
		"SELECT * FROM t WHERE NOT (id > 45)",
		"SELECT * FROM t WHERE NOT (part = 2 AND id > 30)",
		"SELECT * FROM t WHERE NOT (part = 2 OR id > 30)",
		"SELECT * FROM t WHERE NOT (name = 'n-5')",
		// Mixed compounds.
		"SELECT * FROM t WHERE part = 1 AND id > 30",
		"SELECT * FROM t WHERE part = 1 OR id = 2",
		// Timestamps.
		"SELECT * FROM t WHERE ts = TIMESTAMP '2023-11-14 22:13:21+00'",
		"SELECT * FROM t WHERE ts > TIMESTAMP '2023-11-14 22:13:50+00'",
		"SELECT * FROM t WHERE ts < TIMESTAMP '2000-01-01 00:00:00+00'",
		// Filters on projected-away fields.
		"SELECT name FROM t WHERE id > 55",
		"SELECT id FROM t WHERE part = 3",
		// Projection order out of schema order.
		"SELECT name, id FROM t WHERE id BETWEEN 2 AND 4",
		"SELECT COUNT(*) FROM t",
		"SELECT COUNT(*) FROM t WHERE part = 2",
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			got := querySorted(t, push, q)
			want := querySorted(t, full, q)
			if len(got) != len(want) {
				t.Fatalf("row count mismatch: pushdown %d, full scan %d\npush: %v\nfull: %v", len(got), len(want), got, want)
			}
			for i := range got {
				if got[i] != want[i] {
					t.Fatalf("row %d mismatch:\npush: %s\nfull: %s", i, got[i], want[i])
				}
			}
		})
	}
}

func TestScanWithOptions_ProjectionOrderAndValues(t *testing.T) {
	metaLoc := newPartitionedFixture(t)
	p, err := provider.NewTableProvider(context.Background(), metaLoc)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	pd := p.(datafusion.PushdownTableProvider)

	// name (2), id (1): out of schema order.
	reader, err := pd.ScanWithOptions(context.Background(), &datafusion.ScanOptions{
		Projection: []int{2, 1},
		Limit:      -1,
	})
	if err != nil {
		t.Fatalf("ScanWithOptions: %v", err)
	}
	if reader.Schema().NumFields() != 2 || reader.Schema().Field(0).Name != "name" || reader.Schema().Field(1).Name != "id" {
		t.Fatalf("wrong projected order: %v", reader.Schema())
	}
	rows := collectAll(t, reader)
	if len(rows) != 60 {
		t.Fatalf("expected 60 rows, got %d", len(rows))
	}
	sort.Strings(rows)
	// id=1 -> "n-1"; the name column must come first.
	found := false
	for _, r := range rows {
		if r == "|n-1|1" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected row |n-1|1 in %v", rows[:5])
	}

	// Schema-order subset needs no reorder but must still prune fields.
	reader, err = pd.ScanWithOptions(context.Background(), &datafusion.ScanOptions{
		Projection: []int{0, 3},
		Limit:      -1,
	})
	if err != nil {
		t.Fatalf("ScanWithOptions: %v", err)
	}
	if reader.Schema().NumFields() != 2 || reader.Schema().Field(0).Name != "part" || reader.Schema().Field(1).Name != "ts" {
		t.Fatalf("wrong subset schema: %v", reader.Schema())
	}
	if rows := collectAll(t, reader); len(rows) != 60 {
		t.Fatalf("expected 60 rows, got %d", len(rows))
	}
}

func TestScanWithOptions_EmptyProjectionRowCounts(t *testing.T) {
	metaLoc := newPartitionedFixture(t)
	p, _ := provider.NewTableProvider(context.Background(), metaLoc)
	reader, err := p.(datafusion.PushdownTableProvider).ScanWithOptions(context.Background(), &datafusion.ScanOptions{
		Projection: []int{},
		Limit:      -1,
	})
	if err != nil {
		t.Fatalf("ScanWithOptions: %v", err)
	}
	defer reader.Release()
	if reader.Schema().NumFields() != 0 {
		t.Fatalf("expected zero-column schema, got %v", reader.Schema())
	}
	var rows int64
	for reader.Next() {
		rec := reader.RecordBatch()
		if rec.NumCols() != 0 {
			t.Fatalf("expected zero columns, got %d", rec.NumCols())
		}
		rows += rec.NumRows()
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader error: %v", err)
	}
	if rows != 60 {
		t.Fatalf("expected 60 rows, got %d", rows)
	}
}

func TestScanWithOptions_FilterOnProjectedAwayField(t *testing.T) {
	metaLoc := newPartitionedFixture(t)
	p, _ := provider.NewTableProvider(context.Background(), metaLoc)
	reader, err := p.(datafusion.PushdownTableProvider).ScanWithOptions(context.Background(), &datafusion.ScanOptions{
		Projection: []int{2},
		Filters: []datafusion.Expr{datafusion.Compare{
			Column:  datafusion.Column{Name: "id", Index: 1},
			Op:      datafusion.CompareEq,
			Literal: datafusion.Literal{Type: datafusion.LiteralInt64, Value: int64(1)},
		}},
		Limit: -1,
	})
	if err != nil {
		t.Fatalf("ScanWithOptions: %v", err)
	}
	if reader.Schema().NumFields() != 1 || reader.Schema().Field(0).Name != "name" {
		t.Fatalf("filter fields must not leak into the output: %v", reader.Schema())
	}
	rows := collectAll(t, reader)
	if len(rows) != 1 || rows[0] != "|n-1" {
		t.Fatalf("expected exactly |n-1, got %v", rows)
	}
}

func TestScanWithOptions_LimitAndNilOptions(t *testing.T) {
	metaLoc := newPartitionedFixture(t)
	p, _ := provider.NewTableProvider(context.Background(), metaLoc)
	pd := p.(datafusion.PushdownTableProvider)

	reader, err := pd.ScanWithOptions(context.Background(), &datafusion.ScanOptions{Limit: 3})
	if err != nil {
		t.Fatalf("ScanWithOptions: %v", err)
	}
	rows := collectAll(t, reader)
	if len(rows) < 3 {
		t.Fatalf("limit hint must still produce at least 3 rows, got %d", len(rows))
	}

	reader, err = pd.ScanWithOptions(context.Background(), nil)
	if err != nil {
		t.Fatalf("ScanWithOptions(nil): %v", err)
	}
	if rows := collectAll(t, reader); len(rows) != 60 {
		t.Fatalf("nil options must behave like a full scan, got %d rows", len(rows))
	}

	// And through SQL, LIMIT returns exactly 3.
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()
	if err := ctx.RegisterTable("events", pd); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}
	if got := querySorted(t, ctx, "SELECT id FROM events LIMIT 3"); len(got) != 3 {
		t.Fatalf("LIMIT 3 must return 3 rows, got %d", len(got))
	}
}

func TestScanWithOptions_UntranslatableFilterStillCorrect(t *testing.T) {
	// A filter that fails to bind (bogus column name) must be dropped,
	// not fail the scan, and every row must still be produced.
	metaLoc := newPartitionedFixture(t)
	p, _ := provider.NewTableProvider(context.Background(), metaLoc)
	reader, err := p.(datafusion.PushdownTableProvider).ScanWithOptions(context.Background(), &datafusion.ScanOptions{
		Filters: []datafusion.Expr{datafusion.Compare{
			Column:  datafusion.Column{Name: "no_such_column", Index: 0},
			Op:      datafusion.CompareEq,
			Literal: datafusion.Literal{Type: datafusion.LiteralInt64, Value: int64(1)},
		}},
		Limit: -1,
	})
	if err != nil {
		t.Fatalf("a pushed filter must never fail the scan: %v", err)
	}
	if rows := collectAll(t, reader); len(rows) != 60 {
		t.Fatalf("dropped filter must widen to the full scan, got %d rows", len(rows))
	}
}

func TestScan_UnchangedByPushdownSupport(t *testing.T) {
	// The plain Scan path must behave exactly as before: full width, all
	// rows.
	metaLoc := newPartitionedFixture(t)
	p, _ := provider.NewTableProvider(context.Background(), metaLoc)
	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if reader.Schema().NumFields() != 4 {
		t.Fatalf("full scan must keep all fields, got %v", reader.Schema())
	}
	if rows := collectAll(t, reader); len(rows) != 60 {
		t.Fatalf("full scan must return 60 rows, got %d", len(rows))
	}
}
