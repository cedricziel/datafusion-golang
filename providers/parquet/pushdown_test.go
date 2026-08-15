package parquet_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	pqparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/cedricziel/datafusion-golang/datafusion"
	provider "github.com/cedricziel/datafusion-golang/providers/parquet"
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

// collectAll renders every row of the reader as a string, one per row.
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

// diffSchema returns the fixture schema used by the differential battery.
func diffSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "val", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
}

// writeDiffFixture writes a three-row-group file: ids [1,10], [11,20],
// [21,30]; names carry the id parity; val is null for every third row and
// entirely null in the last row group. Stats and bloom filters enabled.
func writeDiffFixture(t *testing.T) string {
	t.Helper()
	sc := diffSchema()
	dir := t.TempDir()
	path := filepath.Join(dir, "diff.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()
	props := pqparquet.NewWriterProperties(pqparquet.WithBloomFilterEnabled(true))
	fw, err := pqarrow.NewFileWriter(sc, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("NewFileWriter: %v", err)
	}
	for g := 0; g < 3; g++ {
		b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
		for i := 0; i < 10; i++ {
			id := int64(g*10 + i + 1)
			b.Field(0).(*array.Int64Builder).Append(id)
			if id%2 == 0 {
				b.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("even-%d", id))
			} else {
				b.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("odd-%d", id))
			}
			if g == 2 || id%3 == 0 {
				b.Field(2).(*array.Float64Builder).AppendNull()
			} else {
				b.Field(2).(*array.Float64Builder).Append(float64(id) / 2)
			}
		}
		rec := b.NewRecordBatch()
		if err := fw.Write(rec); err != nil {
			t.Fatalf("Write group %d: %v", g, err)
		}
		rec.Release()
		b.Release()
	}
	if err := fw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return path
}

func newDiffSessions(t *testing.T, path string) (push, full *datafusion.SessionContext) {
	t.Helper()
	p, err := provider.NewTableProvider(context.Background(), path)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	if _, ok := p.(datafusion.PushdownTableProvider); !ok {
		t.Fatal("parquet provider must implement PushdownTableProvider")
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
	path := writeDiffFixture(t)
	push, full := newDiffSessions(t, path)

	queries := []string{
		"SELECT * FROM t",
		"SELECT * FROM t WHERE id > 15",
		"SELECT * FROM t WHERE id >= 21",
		"SELECT * FROM t WHERE id < 11",
		"SELECT * FROM t WHERE id <= 10",
		"SELECT * FROM t WHERE id = 5",
		"SELECT * FROM t WHERE id = 500",
		"SELECT * FROM t WHERE id <> 7",
		"SELECT * FROM t WHERE id BETWEEN 8 AND 12",
		"SELECT * FROM t WHERE id NOT BETWEEN 5 AND 25",
		"SELECT * FROM t WHERE id IN (3, 17, 300)",
		"SELECT * FROM t WHERE id NOT IN (3, 17)",
		"SELECT * FROM t WHERE val IS NULL",
		"SELECT * FROM t WHERE val IS NOT NULL",
		"SELECT * FROM t WHERE NOT (id > 15)",
		"SELECT * FROM t WHERE NOT (id > 5 AND id < 25)",
		"SELECT * FROM t WHERE id > 25 OR id < 5",
		"SELECT * FROM t WHERE id > 5 AND name = 'even-6'",
		"SELECT * FROM t WHERE name = 'no-such-name'",
		"SELECT * FROM t WHERE name = 'odd-13'",
		"SELECT name, id FROM t WHERE id BETWEEN 2 AND 4",
		"SELECT val FROM t WHERE id > 22",
		"SELECT COUNT(*) FROM t",
		"SELECT COUNT(*) FROM t WHERE id > 15",
		"SELECT * FROM t WHERE val = 2.5",
		"SELECT * FROM t WHERE val > 100.0",
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
	path := writeDiffFixture(t)
	p, err := provider.NewTableProvider(context.Background(), path)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	pd := p.(datafusion.PushdownTableProvider)

	// Projection out of file order: name (1), id (0).
	reader, err := pd.ScanWithOptions(context.Background(), &datafusion.ScanOptions{
		Projection: []int{1, 0},
		Limit:      -1,
	})
	if err != nil {
		t.Fatalf("ScanWithOptions: %v", err)
	}
	if got := reader.Schema().NumFields(); got != 2 {
		t.Fatalf("expected 2 fields, got %d", got)
	}
	if reader.Schema().Field(0).Name != "name" || reader.Schema().Field(1).Name != "id" {
		t.Fatalf("wrong projected order: %v", reader.Schema())
	}
	rows := collectAll(t, reader)
	if len(rows) != 30 {
		t.Fatalf("expected 30 rows, got %d", len(rows))
	}
	if rows[0] != "|odd-1|1" {
		t.Fatalf("unexpected first row: %s", rows[0])
	}
}

func TestScanWithOptions_DuplicateProjection(t *testing.T) {
	path := writeDiffFixture(t)
	pd, _ := provider.NewTableProvider(context.Background(), path)
	reader, err := pd.(datafusion.PushdownTableProvider).ScanWithOptions(context.Background(), &datafusion.ScanOptions{
		Projection: []int{0, 0},
		Limit:      -1,
	})
	if err != nil {
		t.Fatalf("ScanWithOptions: %v", err)
	}
	if reader.Schema().NumFields() != 2 || reader.Schema().Field(0).Name != "id" || reader.Schema().Field(1).Name != "id" {
		t.Fatalf("wrong duplicated projection schema: %v", reader.Schema())
	}
	rows := collectAll(t, reader)
	if len(rows) != 30 || rows[0] != "|1|1" {
		t.Fatalf("unexpected duplicated projection rows: %d rows, first %q", len(rows), rows[0])
	}
}

func TestScanWithOptions_EmptyProjectionRowCounts(t *testing.T) {
	path := writeDiffFixture(t)
	pd, _ := provider.NewTableProvider(context.Background(), path)
	reader, err := pd.(datafusion.PushdownTableProvider).ScanWithOptions(context.Background(), &datafusion.ScanOptions{
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
	if rows != 30 {
		t.Fatalf("expected 30 rows from empty projection, got %d", rows)
	}
}

func TestScanWithOptions_AllRowGroupsSkipped(t *testing.T) {
	path := writeDiffFixture(t)
	pd, _ := provider.NewTableProvider(context.Background(), path)
	reader, err := pd.(datafusion.PushdownTableProvider).ScanWithOptions(context.Background(), &datafusion.ScanOptions{
		Projection: []int{1},
		Filters: []datafusion.Expr{datafusion.Compare{
			Column:  datafusion.Column{Name: "id", Index: 0},
			Op:      datafusion.CompareGt,
			Literal: datafusion.Literal{Type: datafusion.LiteralInt64, Value: int64(1000)},
		}},
		Limit: -1,
	})
	if err != nil {
		t.Fatalf("ScanWithOptions: %v", err)
	}
	if reader.Schema().NumFields() != 1 || reader.Schema().Field(0).Name != "name" {
		t.Fatalf("empty result must keep the projected schema, got %v", reader.Schema())
	}
	if rows := collectAll(t, reader); len(rows) != 0 {
		t.Fatalf("expected no rows, got %v", rows)
	}
}

func TestScanWithOptions_LimitStopsEarly(t *testing.T) {
	// 3 row groups x 1000 rows: the reader batches at 1024 rows, so a
	// limit of 3 must stop after the first batch instead of draining all
	// 3000 rows.
	sc := diffSchema()
	dir := t.TempDir()
	path := filepath.Join(dir, "big.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	fw, err := pqarrow.NewFileWriter(sc, f, pqparquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("NewFileWriter: %v", err)
	}
	for g := 0; g < 3; g++ {
		b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
		for i := 0; i < 1000; i++ {
			b.Field(0).(*array.Int64Builder).Append(int64(g*1000 + i))
			b.Field(1).(*array.StringBuilder).Append("n")
			b.Field(2).(*array.Float64Builder).Append(1.5)
		}
		rec := b.NewRecordBatch()
		if err := fw.Write(rec); err != nil {
			t.Fatalf("Write: %v", err)
		}
		rec.Release()
		b.Release()
	}
	if err := fw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	f.Close()

	pd, err := provider.NewTableProvider(context.Background(), path)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	reader, err := pd.(datafusion.PushdownTableProvider).ScanWithOptions(context.Background(), &datafusion.ScanOptions{Limit: 3})
	if err != nil {
		t.Fatalf("ScanWithOptions: %v", err)
	}
	rows := collectAll(t, reader)
	if len(rows) < 3 {
		t.Fatalf("limit hint must still produce at least 3 rows, got %d", len(rows))
	}
	if len(rows) >= 3000 {
		t.Fatalf("limit hint did not stop the scan early: got all %d rows", len(rows))
	}

	// And through SQL, the query returns exactly 3.
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()
	if err := ctx.RegisterTable("big", pd); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}
	got := querySorted(t, ctx, "SELECT id FROM big LIMIT 3")
	if len(got) != 3 {
		t.Fatalf("LIMIT 3 must return 3 rows, got %d", len(got))
	}
}

func TestScanWithOptions_NilOptionsAndEmptyFile(t *testing.T) {
	dir := t.TempDir()
	sc := diffSchema()
	path := filepath.Join(dir, "empty.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	fw, err := pqarrow.NewFileWriter(sc, f, pqparquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("NewFileWriter: %v", err)
	}
	if err := fw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	f.Close()

	pd, err := provider.NewTableProvider(context.Background(), path)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	reader, err := pd.(datafusion.PushdownTableProvider).ScanWithOptions(context.Background(), &datafusion.ScanOptions{
		Projection: []int{0},
		Filters: []datafusion.Expr{datafusion.Compare{
			Column:  datafusion.Column{Name: "id", Index: 0},
			Op:      datafusion.CompareEq,
			Literal: datafusion.Literal{Type: datafusion.LiteralInt64, Value: int64(1)},
		}},
		Limit: -1,
	})
	if err != nil {
		t.Fatalf("ScanWithOptions on empty file: %v", err)
	}
	if rows := collectAll(t, reader); len(rows) != 0 {
		t.Fatalf("expected no rows, got %v", rows)
	}

	reader, err = pd.(datafusion.PushdownTableProvider).ScanWithOptions(context.Background(), nil)
	if err != nil {
		t.Fatalf("ScanWithOptions(nil): %v", err)
	}
	if rows := collectAll(t, reader); len(rows) != 0 {
		t.Fatalf("expected no rows, got %v", rows)
	}
}
