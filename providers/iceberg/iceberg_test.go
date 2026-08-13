package iceberg_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/hadoop"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/cedricziel/datafusion-golang/datafusion"
	provider "github.com/cedricziel/datafusion-golang/providers/iceberg"
)

func peopleSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

func peopleBatch(t *testing.T, schema *arrow.Schema, ids []int64, names []string) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	b.Field(1).(*array.StringBuilder).AppendValues(names, nil)
	return b.NewRecordBatch()
}

// newIcebergCatalogFixture creates a real local-filesystem Iceberg table
// (via iceberg-go's own Hadoop catalog and write path, no hand-authored
// metadata) with one Append per batch, so batches map onto separate data
// files in the current snapshot. It returns the catalog itself and the
// table's namespace-qualified identifier, so tests can exercise either the
// metadata.json path (via cat.LoadTable(...).MetadataLocation()) or the
// catalog.Catalog path (NewTableProviderFromCatalog) against the same
// fixture.
func newIcebergCatalogFixture(t *testing.T, tableName string, schema *arrow.Schema, batches ...arrow.RecordBatch) (cat catalog.Catalog, identifier []string) {
	t.Helper()
	ctx := context.Background()

	warehouse := t.TempDir()
	hcat, err := hadoop.NewCatalog("test", warehouse, nil)
	if err != nil {
		t.Fatalf("hadoop.NewCatalog: %v", err)
	}
	if err := hcat.CreateNamespace(ctx, []string{"default"}, nil); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}

	icebergSchema, err := icebergtable.ArrowSchemaToIcebergWithFreshIDs(schema, false)
	if err != nil {
		t.Fatalf("ArrowSchemaToIcebergWithFreshIDs: %v", err)
	}

	ident := []string{"default", tableName}
	tbl, err := hcat.CreateTable(ctx, ident, icebergSchema)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	for i, batch := range batches {
		batch.Retain()
		rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{batch})
		if err != nil {
			t.Fatalf("NewRecordReader for batch %d: %v", i, err)
		}
		next, err := tbl.Append(ctx, rdr, nil)
		rdr.Release()
		if err != nil {
			t.Fatalf("Append batch %d: %v", i, err)
		}
		tbl = next
	}

	return hcat, ident
}

// newIcebergFixture is newIcebergCatalogFixture for callers that only need
// a metadata.json location (the direct-path construction tests).
func newIcebergFixture(t *testing.T, tableName string, schema *arrow.Schema, batches ...arrow.RecordBatch) string {
	t.Helper()
	cat, ident := newIcebergCatalogFixture(t, tableName, schema, batches...)
	tbl, err := cat.LoadTable(context.Background(), ident)
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	return tbl.MetadataLocation()
}

func collectRows(t *testing.T, reader array.RecordReader) (ids []int64, names []string) {
	t.Helper()
	defer reader.Release()
	for reader.Next() {
		rec := reader.RecordBatch()
		idCol := rec.Column(0).(*array.Int64)
		nameCol := rec.Column(1).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			ids = append(ids, idCol.Value(i))
			names = append(names, strings.Clone(nameCol.Value(i)))
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reading result: %v", err)
	}
	return ids, names
}

func TestNewTableProvider_ValidTable(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()
	metaLoc := newIcebergFixture(t, "people", schema, batch)

	p, err := provider.NewTableProvider(context.Background(), metaLoc)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	if p.Schema() == nil {
		t.Fatalf("expected non-nil schema")
	}
	if got, want := p.Schema().NumFields(), 2; got != want {
		t.Fatalf("expected %d fields, got %d", want, got)
	}
	if p.Schema().Field(0).Name != "id" || p.Schema().Field(1).Name != "name" {
		t.Fatalf("unexpected field names: %v", p.Schema())
	}
}

func TestNewTableProvider_MissingMetadata(t *testing.T) {
	_, err := provider.NewTableProvider(context.Background(), filepath.Join(t.TempDir(), "does-not-exist", "metadata.json"))
	if err == nil {
		t.Fatalf("expected an error for a missing metadata location")
	}
}

func TestNewTableProvider_InvalidMetadata(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "metadata.json")
	if err := os.WriteFile(path, []byte("this is not valid iceberg metadata"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := provider.NewTableProvider(context.Background(), path)
	if err == nil {
		t.Fatalf("expected an error for invalid metadata")
	}
}

func TestScan_MultipleDataFiles(t *testing.T) {
	schema := peopleSchema()
	b1 := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer b1.Release()
	b2 := peopleBatch(t, schema, []int64{3, 4}, []string{"carol", "dave"})
	defer b2.Release()
	metaLoc := newIcebergFixture(t, "people", schema, b1, b2)

	p, err := provider.NewTableProvider(context.Background(), metaLoc)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	ids, names := collectRows(t, reader)
	if len(ids) != 4 {
		t.Fatalf("expected 4 rows across data files, got %v", ids)
	}
	sum := int64(0)
	for _, id := range ids {
		sum += id
	}
	if sum != 1+2+3+4 {
		t.Fatalf("unexpected ids: %v", ids)
	}
	foundAll := map[string]bool{"alice": false, "bob": false, "carol": false, "dave": false}
	for _, n := range names {
		foundAll[n] = true
	}
	for name, found := range foundAll {
		if !found {
			t.Fatalf("expected to find %q among scanned rows: %v", name, names)
		}
	}
}

func TestScan_EmptySnapshot(t *testing.T) {
	schema := peopleSchema()
	metaLoc := newIcebergFixture(t, "people", schema) // zero batches -> no snapshot at all

	p, err := provider.NewTableProvider(context.Background(), metaLoc)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer reader.Release()
	if reader.Schema().NumFields() != 2 {
		t.Fatalf("expected schema with 2 fields even for an empty snapshot")
	}
	for reader.Next() {
		t.Fatalf("expected no rows from an empty snapshot")
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScan_CorruptDataFileSurfacesAsReaderError(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()
	metaLoc := newIcebergFixture(t, "people", schema, batch)

	// Corrupt every Parquet data file referenced by the table so the scan
	// must fail instead of silently returning zero/partial rows.
	dataDir := filepath.Join(filepath.Dir(filepath.Dir(metaLoc)), "data")
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatalf("ReadDir data dir %s: %v", dataDir, err)
	}
	if len(entries) == 0 {
		t.Fatalf("expected at least one data file under %s", dataDir)
	}
	for _, e := range entries {
		if err := os.WriteFile(filepath.Join(dataDir, e.Name()), []byte("not a parquet file"), 0o644); err != nil {
			t.Fatalf("corrupt data file %s: %v", e.Name(), err)
		}
	}

	p, err := provider.NewTableProvider(context.Background(), metaLoc)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	reader, err := p.Scan(context.Background())
	if err != nil {
		// Failing at Scan-open time also satisfies "surfaces as a Go error".
		return
	}
	defer reader.Release()
	for reader.Next() {
		// drain
	}
	if reader.Err() == nil {
		t.Fatalf("expected a reader error when the data file is corrupt")
	}
}

// openFDCount counts this process's open file descriptors via `lsof -p`,
// matching the approach used in providers/parquet's leak test.
func openFDCount(t *testing.T) int {
	t.Helper()
	out, err := exec.Command("lsof", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		t.Skipf("cannot count open file descriptors via lsof: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(out), "\n"), "\n")
	if len(lines) == 0 {
		return 0
	}
	return len(lines) - 1
}

func TestScan_RepeatedSequentialScansDoNotLeakFileDescriptors(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()
	metaLoc := newIcebergFixture(t, "people", schema, batch)

	p, err := provider.NewTableProvider(context.Background(), metaLoc)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}

	before := openFDCount(t)
	for i := 0; i < 100; i++ {
		reader, err := p.Scan(context.Background())
		if err != nil {
			t.Fatalf("Scan iteration %d: %v", i, err)
		}
		ids, _ := collectRows(t, reader)
		if len(ids) != 2 {
			t.Fatalf("iteration %d: expected 2 rows, got %d", i, len(ids))
		}
	}
	after := openFDCount(t)
	if after > before+4 {
		t.Fatalf("file descriptor leak: before=%d after=%d open fds", before, after)
	}
}

func TestScan_ConcurrentScansEachReturnFullResults(t *testing.T) {
	schema := peopleSchema()
	b1 := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer b1.Release()
	b2 := peopleBatch(t, schema, []int64{3, 4}, []string{"carol", "dave"})
	defer b2.Release()
	metaLoc := newIcebergFixture(t, "people", schema, b1, b2)

	p, err := provider.NewTableProvider(context.Background(), metaLoc)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader, err := p.Scan(context.Background())
			if err != nil {
				errs <- err
				return
			}
			ids, _ := collectRows(t, reader)
			if len(ids) != 4 {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("goroutine error: %v", err)
		}
	}
}

func TestRegisterAndQueryViaSQL(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()
	metaLoc := newIcebergFixture(t, "people", schema, batch)

	p, err := provider.NewTableProvider(context.Background(), metaLoc)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}

	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	if err := ctx.RegisterTable("people", p); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	reader, err := ctx.SQL("SELECT id, name FROM people ORDER BY id")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	ids, names := collectRows(t, reader)
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("unexpected ids: %v", ids)
	}
	if names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("unexpected names: %v", names)
	}
}
