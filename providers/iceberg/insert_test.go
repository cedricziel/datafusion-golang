package iceberg_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	ib "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/table"
	"github.com/cedricziel/datafusion-golang/datafusion"
	provider "github.com/cedricziel/datafusion-golang/providers/iceberg"
)

// newPartitionedWritableFixture builds a real Iceberg table
// identity-partitioned on "part", seeded with one row per partition in
// 1..3, and returns the catalog and identifier so tests can insert against
// it through NewTableProviderFromCatalog.
func newPartitionedWritableFixture(t *testing.T) (cat catalog.Catalog, ident []string) {
	t.Helper()
	ctx := context.Background()
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "part", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	warehouse := t.TempDir()
	hcat, err := hadoop.NewCatalog("test", warehouse, nil)
	if err != nil {
		t.Fatalf("hadoop.NewCatalog: %v", err)
	}
	if err := hcat.CreateNamespace(ctx, []string{"default"}, nil); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	icebergSchema, err := table.ArrowSchemaToIcebergWithFreshIDs(sc, false)
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
	ident = []string{"default", "events"}
	tbl, err := hcat.CreateTable(ctx, ident, icebergSchema, catalog.WithPartitionSpec(&spec))
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	seed := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	for p := int64(1); p <= 3; p++ {
		seed.Field(0).(*array.Int64Builder).Append(p)
		seed.Field(1).(*array.Int64Builder).Append(p * 100)
	}
	rec := seed.NewRecordBatch()
	seed.Release()
	rdr, err := array.NewRecordReader(sc, []arrow.RecordBatch{rec})
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	if _, err := tbl.Append(ctx, rdr, nil); err != nil {
		rdr.Release()
		rec.Release()
		t.Fatalf("seed Append: %v", err)
	}
	rdr.Release()
	rec.Release()

	return hcat, ident
}

// errAfterReader wraps a RecordReader and fails after successfully
// yielding failAfter batches, so tests can inject a mid-stream failure at
// a specific point without a real broken data source.
type errAfterReader struct {
	array.RecordReader
	failAfter int
	calls     int
	err       error
}

func (r *errAfterReader) Next() bool {
	if r.calls >= r.failAfter {
		r.err = errors.New("injected read failure")
		return false
	}
	ok := r.RecordReader.Next()
	if ok {
		r.calls++
	}
	return ok
}

func (r *errAfterReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.RecordReader.Err()
}

func newRowsReader(t *testing.T, schema *arrow.Schema, batches ...arrow.RecordBatch) array.RecordReader {
	t.Helper()
	for _, b := range batches {
		b.Retain()
	}
	rdr, err := array.NewRecordReader(schema, batches)
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	return rdr
}

func newWritableProvider(t *testing.T, cat catalog.Catalog, ident []string) interface {
	datafusion.TableProvider
	datafusion.WritableTableProvider
} {
	t.Helper()
	p, err := provider.NewTableProviderFromCatalog(context.Background(), cat, ident...)
	if err != nil {
		t.Fatalf("NewTableProviderFromCatalog: %v", err)
	}
	writable, ok := p.(interface {
		datafusion.TableProvider
		datafusion.WritableTableProvider
	})
	if !ok {
		t.Fatalf("catalog-backed iceberg provider does not implement WritableTableProvider")
	}
	return writable
}

func currentSnapshotID(t *testing.T, cat catalog.Catalog, ident []string) int64 {
	t.Helper()
	tbl, err := cat.LoadTable(context.Background(), ident)
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return -1
	}
	return snap.SnapshotID
}

// failingCommitCatalog wraps a real catalog.Catalog so that tables loaded
// through it commit against itself (LoadTable rebuilds the Table with
// itself as the CatalogIO, matching what hadoop.Catalog.LoadTable does
// internally) while CommitTable always fails — simulating a catalog-side
// commit rejection (e.g. a conflicting concurrent writer) independent of
// whether the write itself succeeded.
type failingCommitCatalog struct {
	catalog.Catalog
	metadataLocation string
	commitErr        error
}

func (f *failingCommitCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	tbl, err := f.Catalog.LoadTable(ctx, ident)
	if err != nil {
		return nil, err
	}
	f.metadataLocation = tbl.MetadataLocation()
	return table.NewFromLocation(ctx, ident, tbl.MetadataLocation(), tbl.FS, f)
}

func (f *failingCommitCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	return nil, "", f.commitErr
}

func TestInsertInto_RejectsReplace(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)
	before := currentSnapshotID(t, cat, ident)

	p := newWritableProvider(t, cat, ident)
	rows := newRowsReader(t, schema, peopleBatch(t, schema, []int64{2}, []string{"bob"}))
	if _, err := p.InsertInto(context.Background(), datafusion.InsertReplace, rows); err == nil {
		t.Fatalf("expected Replace to be rejected")
	}
	if got := currentSnapshotID(t, cat, ident); got != before {
		t.Fatalf("snapshot must be unchanged by a rejected insert: before=%d after=%d", before, got)
	}
}

func TestNewTableProvider_NotWritable(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	metaLoc := newIcebergFixture(t, "people", schema, batch)

	p, err := provider.NewTableProvider(context.Background(), metaLoc)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	if _, ok := p.(datafusion.WritableTableProvider); ok {
		t.Fatalf("a metadata-location provider must not implement WritableTableProvider (design D4): no catalog means no commit channel")
	}
}

func TestNewTableProviderFromCatalog_IsWritable(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)

	p, err := provider.NewTableProviderFromCatalog(context.Background(), cat, ident...)
	if err != nil {
		t.Fatalf("NewTableProviderFromCatalog: %v", err)
	}
	if _, ok := p.(datafusion.WritableTableProvider); !ok {
		t.Fatalf("a catalog-backed provider must implement WritableTableProvider (design D4)")
	}
}

func TestInsertInto_FailingReaderLeavesSnapshotUnchanged(t *testing.T) {
	for _, failAfter := range []int{0, 1} {
		// A named (not "") subtest: an anonymous subtest's t.TempDir()
		// path gets a literal "#00" disambiguator, and the hadoop
		// catalog parses warehouse locations as URIs where "#" is the
		// fragment delimiter — truncating the path and breaking
		// namespace creation.
		t.Run(fmt.Sprintf("failAfter=%d", failAfter), func(t *testing.T) {
			schema := peopleSchema()
			batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
			defer batch.Release()
			cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)
			before := currentSnapshotID(t, cat, ident)

			p := newWritableProvider(t, cat, ident)
			b1 := peopleBatch(t, schema, []int64{2}, []string{"bob"})
			defer b1.Release()
			b2 := peopleBatch(t, schema, []int64{3}, []string{"carol"})
			defer b2.Release()
			inner := newRowsReader(t, schema, b1, b2)
			rows := &errAfterReader{RecordReader: inner, failAfter: failAfter}

			if _, err := p.InsertInto(context.Background(), datafusion.InsertAppend, rows); err == nil {
				t.Fatalf("expected the injected read failure to abort the insert")
			}
			if got := currentSnapshotID(t, cat, ident); got != before {
				t.Fatalf("snapshot must be unchanged after a failed insert (failAfter=%d): before=%d after=%d", failAfter, before, got)
			}

			reader, err := p.Scan(context.Background())
			if err != nil {
				t.Fatalf("Scan: %v", err)
			}
			ids, _ := collectRows(t, reader)
			if len(ids) != 1 || ids[0] != 1 {
				t.Fatalf("expected only the pre-insert row after a failed insert, got %v", ids)
			}
		})
	}
}

func TestInsertInto_CommitFailureLeavesTableUnchanged(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	realCat, ident := newIcebergCatalogFixture(t, "people", schema, batch)
	before := currentSnapshotID(t, realCat, ident)

	failing := &failingCommitCatalog{Catalog: realCat, commitErr: errors.New("commit rejected")}
	p := newWritableProvider(t, failing, ident)

	rows := newRowsReader(t, schema, peopleBatch(t, schema, []int64{2}, []string{"bob"}))
	if _, err := p.InsertInto(context.Background(), datafusion.InsertAppend, rows); err == nil {
		t.Fatalf("expected the commit failure to fail the insert")
	}

	if got := currentSnapshotID(t, realCat, ident); got != before {
		t.Fatalf("a rejected commit must leave the table's snapshot unchanged: before=%d after=%d", before, got)
	}
	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	ids, _ := collectRows(t, reader)
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("expected only the pre-insert row after a rejected commit, got %v", ids)
	}
}

func TestInsertInto_AppendThenScanReturnsCombinedRows(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)

	p := newWritableProvider(t, cat, ident)
	rows := newRowsReader(t, schema, peopleBatch(t, schema, []int64{2}, []string{"bob"}))
	count, err := p.InsertInto(context.Background(), datafusion.InsertAppend, rows)
	if err != nil {
		t.Fatalf("InsertInto: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count=1, got %d", count)
	}

	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	ids, names := collectRows(t, reader)
	if len(ids) != 2 {
		t.Fatalf("expected 2 combined rows, got %v", ids)
	}
	found := map[string]bool{}
	for _, n := range names {
		found[n] = true
	}
	if !found["alice"] || !found["bob"] {
		t.Fatalf("expected both alice and bob, got %v", names)
	}
}

func TestInsertInto_OverwriteThenScanReturnsReplacedRows(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)

	p := newWritableProvider(t, cat, ident)
	rows := newRowsReader(t, schema, peopleBatch(t, schema, []int64{9}, []string{"zed"}))
	count, err := p.InsertInto(context.Background(), datafusion.InsertOverwrite, rows)
	if err != nil {
		t.Fatalf("InsertInto: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count=1, got %d", count)
	}

	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	ids, names := collectRows(t, reader)
	if len(ids) != 1 || ids[0] != 9 || names[0] != "zed" {
		t.Fatalf("expected only (9, zed) after overwrite, got ids=%v names=%v", ids, names)
	}
}

func TestInsertInto_ZeroRowAppendSkipsCommit(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)
	before := currentSnapshotID(t, cat, ident)

	p := newWritableProvider(t, cat, ident)
	rows := newRowsReader(t, schema)
	count, err := p.InsertInto(context.Background(), datafusion.InsertAppend, rows)
	if err != nil {
		t.Fatalf("InsertInto: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected count=0, got %d", count)
	}
	if got := currentSnapshotID(t, cat, ident); got != before {
		t.Fatalf("a zero-row Append must not create a new snapshot: before=%d after=%d", before, got)
	}
}

func TestInsertInto_ZeroRowOverwriteCommitsEmptyTable(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)
	before := currentSnapshotID(t, cat, ident)

	p := newWritableProvider(t, cat, ident)
	rows := newRowsReader(t, schema)
	count, err := p.InsertInto(context.Background(), datafusion.InsertOverwrite, rows)
	if err != nil {
		t.Fatalf("InsertInto: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected count=0, got %d", count)
	}
	if got := currentSnapshotID(t, cat, ident); got == before {
		t.Fatalf("a zero-row Overwrite must still commit a new (empty) snapshot")
	}

	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	ids, _ := collectRows(t, reader)
	if len(ids) != 0 {
		t.Fatalf("expected zero rows after zero-row Overwrite, got %v", ids)
	}
}

// TestInsertInto_FieldIDMetadataOnInputIsIgnored guards design D6: batches
// arriving with field-id metadata on none, some, or all fields must all
// insert correctly, since the provider always rebinds them onto its own
// registered schema (which never carries field ids) before handing them to
// iceberg-go — a partial id set would otherwise be a conversion error.
func TestInsertInto_FieldIDMetadataOnInputIsIgnored(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)

	withFieldID := func(name string, typ arrow.DataType, id string) arrow.Field {
		f := arrow.Field{Name: name, Type: typ, Nullable: true}
		if id != "" {
			f.Metadata = arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{id})
		}
		return f
	}

	cases := map[string]*arrow.Schema{
		"full":    arrow.NewSchema([]arrow.Field{withFieldID("id", arrow.PrimitiveTypes.Int64, "1"), withFieldID("name", arrow.BinaryTypes.String, "2")}, nil),
		"partial": arrow.NewSchema([]arrow.Field{withFieldID("id", arrow.PrimitiveTypes.Int64, "1"), withFieldID("name", arrow.BinaryTypes.String, "")}, nil),
		"none":    arrow.NewSchema([]arrow.Field{withFieldID("id", arrow.PrimitiveTypes.Int64, ""), withFieldID("name", arrow.BinaryTypes.String, "")}, nil),
	}

	for name, taggedSchema := range cases {
		t.Run(name, func(t *testing.T) {
			p := newWritableProvider(t, cat, ident)
			b := array.NewRecordBuilder(memory.DefaultAllocator, taggedSchema)
			b.Field(0).(*array.Int64Builder).Append(2)
			b.Field(1).(*array.StringBuilder).Append("bob")
			rec := b.NewRecordBatch()
			b.Release()
			rows := newRowsReader(t, taggedSchema, rec)
			rec.Release()

			count, err := p.InsertInto(context.Background(), datafusion.InsertOverwrite, rows)
			if err != nil {
				t.Fatalf("InsertInto with %s field-id metadata: %v", name, err)
			}
			if count != 1 {
				t.Fatalf("expected count=1, got %d", count)
			}

			reader, err := p.Scan(context.Background())
			if err != nil {
				t.Fatalf("Scan: %v", err)
			}
			ids, names := collectRows(t, reader)
			if len(ids) != 1 || ids[0] != 2 || names[0] != "bob" {
				t.Fatalf("unexpected rows after %s-field-id insert: ids=%v names=%v", name, ids, names)
			}
		})
	}
}

func TestInsertInto_PartitionedAppendThenPartitionFilteredScan(t *testing.T) {
	cat, ident := newPartitionedWritableFixture(t)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "part", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	p := newWritableProvider(t, cat, ident)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	b.Field(0).(*array.Int64Builder).Append(2)
	b.Field(1).(*array.Int64Builder).Append(201)
	rec := b.NewRecordBatch()
	b.Release()
	rows := newRowsReader(t, schema, rec)
	rec.Release()

	count, err := p.InsertInto(context.Background(), datafusion.InsertAppend, rows)
	if err != nil {
		t.Fatalf("InsertInto: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count=1, got %d", count)
	}

	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()
	if err := ctx.RegisterTable("events", p); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}
	reader, err := ctx.SQL("SELECT id FROM events WHERE part = 2 ORDER BY id")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	var ids []int64
	for reader.Next() {
		col := reader.RecordBatch().Column(0).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			ids = append(ids, col.Value(i))
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reading result: %v", err)
	}
	reader.Release()
	if len(ids) != 2 || ids[0] != 200 || ids[1] != 201 {
		t.Fatalf("expected partition 2 to contain [200, 201] after the append, got %v", ids)
	}
}

func TestInsertInto_ConcurrentAppendsBothLand(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)

	p := newWritableProvider(t, cat, ident)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, row := range [][2]any{{int64(2), "bob"}, {int64(3), "carol"}} {
		wg.Go(func() {
			rows := newRowsReader(t, schema, peopleBatch(t, schema, []int64{row[0].(int64)}, []string{row[1].(string)}))
			if _, err := p.InsertInto(context.Background(), datafusion.InsertAppend, rows); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent insert error: %v", err)
	}

	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	ids, _ := collectRows(t, reader)
	if len(ids) != 3 {
		t.Fatalf("expected 3 rows (1 seed + 2 concurrent appends), got %d: %v", len(ids), ids)
	}
}

// TestScan_ConcurrentWithInsert_SeesConsistentSnapshot guards design D7's
// Iceberg claim: a scan is snapshot-isolated by construction (it takes a
// fresh table handle and iterates that snapshot's data files), so a scan
// racing an insert's commit must never see a torn mix of old and new rows
// — only the pre-commit or the post-commit row count, never anything else.
func TestScan_ConcurrentWithInsert_SeesConsistentSnapshot(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)

	p := newWritableProvider(t, cat, ident)
	var wg sync.WaitGroup
	scanCounts := make(chan int, 20)
	for range 20 {
		wg.Go(func() {
			reader, err := p.Scan(context.Background())
			if err != nil {
				t.Errorf("Scan: %v", err)
				return
			}
			ids, _ := collectRows(t, reader)
			scanCounts <- len(ids)
		})
	}
	rows := newRowsReader(t, schema, peopleBatch(t, schema, []int64{2}, []string{"bob"}))
	if _, err := p.InsertInto(context.Background(), datafusion.InsertAppend, rows); err != nil {
		t.Fatalf("InsertInto: %v", err)
	}
	wg.Wait()
	close(scanCounts)

	for n := range scanCounts {
		if n != 1 && n != 2 {
			t.Fatalf("scan raced with insert commit saw a torn row count: %d (want 1 or 2)", n)
		}
	}
}
