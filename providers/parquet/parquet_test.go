package parquet_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	pqparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/cedricziel/datafusion-golang/datafusion"
	"github.com/cedricziel/datafusion-golang/objectstore"
	provider "github.com/cedricziel/datafusion-golang/providers/parquet"
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

// writeParquetFile writes batches to a new Parquet file under dir, starting
// a fresh row group before each batch after the first so multi-row-group
// fixtures are easy to construct.
func writeParquetFile(t *testing.T, dir, name string, schema *arrow.Schema, batches ...arrow.RecordBatch) string {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()

	fw, err := pqarrow.NewFileWriter(schema, f, pqparquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("NewFileWriter: %v", err)
	}
	// Write always starts a new row group, so writing each batch in its
	// own call is enough to produce one row group per batch.
	for i, rec := range batches {
		if err := fw.Write(rec); err != nil {
			t.Fatalf("Write batch %d: %v", i, err)
		}
	}
	if err := fw.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return path
}

// writeParquetToStore writes batches to a Parquet object at location
// through the objectstore package, mirroring writeParquetFile for
// locations on any registered backend (e.g. mem://).
func writeParquetToStore(t *testing.T, location string, schema *arrow.Schema, batches ...arrow.RecordBatch) {
	t.Helper()
	ctx := context.Background()
	store, path, err := objectstore.Resolve(ctx, location)
	if err != nil {
		t.Fatalf("Resolve %s: %v", location, err)
	}
	w, err := store.Create(ctx, path)
	if err != nil {
		t.Fatalf("Create %s: %v", location, err)
	}
	fw, err := pqarrow.NewFileWriter(schema, w, pqparquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("NewFileWriter: %v", err)
	}
	for i, rec := range batches {
		if err := fw.Write(rec); err != nil {
			t.Fatalf("Write batch %d: %v", i, err)
		}
	}
	if err := fw.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
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

// TestNewTableProvider_ValidObjectOnRegisteredBackend covers the
// object-store spec's "Valid object on a registered backend" scenario:
// construction and querying work identically over a mem:// location, not
// just a bare local path.
func TestNewTableProvider_ValidObjectOnRegisteredBackend(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()
	location := "mem://" + t.Name() + "/people.parquet"
	writeParquetToStore(t, location, schema, batch)

	p, err := provider.NewTableProvider(context.Background(), location)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}

	sess, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer sess.Close()
	if err := sess.RegisterTable("people", p); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	reader, err := sess.SQL("SELECT id, name FROM people ORDER BY id")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	ids, names := collectRows(t, reader)
	if !reflect.DeepEqual(ids, []int64{1, 2}) || !reflect.DeepEqual(names, []string{"alice", "bob"}) {
		t.Fatalf("got ids=%v names=%v, want [1 2] [alice bob]", ids, names)
	}
}

func TestNewTableProvider_ValidFile(t *testing.T) {
	dir := t.TempDir()
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()
	path := writeParquetFile(t, dir, "people.parquet", schema, batch)

	p, err := provider.NewTableProvider(context.Background(), path)
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

func TestNewTableProvider_MissingFile(t *testing.T) {
	_, err := provider.NewTableProvider(context.Background(), filepath.Join(t.TempDir(), "does-not-exist.parquet"))
	if err == nil {
		t.Fatalf("expected an error for a missing file")
	}
}

func TestNewTableProvider_InvalidFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "not-parquet.parquet")
	if err := os.WriteFile(path, []byte("this is not a parquet file"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := provider.NewTableProvider(context.Background(), path)
	if err == nil {
		t.Fatalf("expected an error for an invalid parquet file")
	}
}

func TestScan_MultiRowGroup(t *testing.T) {
	dir := t.TempDir()
	schema := peopleSchema()
	b1 := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer b1.Release()
	b2 := peopleBatch(t, schema, []int64{3, 4}, []string{"carol", "dave"})
	defer b2.Release()
	path := writeParquetFile(t, dir, "people.parquet", schema, b1, b2)

	p, err := provider.NewTableProvider(context.Background(), path)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	ids, names := collectRows(t, reader)
	if len(ids) != 4 {
		t.Fatalf("expected 4 rows across row groups, got %v", ids)
	}
	for i, want := range []int64{1, 2, 3, 4} {
		if ids[i] != want {
			t.Fatalf("row %d: expected id %d, got %d", i, want, ids[i])
		}
	}
	if names[0] != "alice" || names[3] != "dave" {
		t.Fatalf("unexpected names: %v", names)
	}
}

func TestScan_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	schema := peopleSchema()
	path := writeParquetFile(t, dir, "empty.parquet", schema)

	p, err := provider.NewTableProvider(context.Background(), path)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer reader.Release()
	if reader.Schema().NumFields() != 2 {
		t.Fatalf("expected schema with 2 fields even for an empty file")
	}
	for reader.Next() {
		t.Fatalf("expected no rows from an empty file")
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// openFDCount counts this process's open file descriptors via `lsof -p`,
// which works uniformly on macOS and Linux (unlike /dev/fd, whose
// directory-listing semantics are unreliable on macOS). Used to detect fd
// leaks directly rather than relying on eventually hitting ulimit.
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
	return len(lines) - 1 // first line is the lsof header
}

func TestScan_RepeatedSequentialScansDoNotLeakFileDescriptors(t *testing.T) {
	dir := t.TempDir()
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()
	path := writeParquetFile(t, dir, "people.parquet", schema, batch)

	p, err := provider.NewTableProvider(context.Background(), path)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}

	before := openFDCount(t)
	for i := 0; i < 300; i++ {
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
	if after > before+2 {
		t.Fatalf("file descriptor leak: before=%d after=%d open fds", before, after)
	}
}

func TestScan_ConcurrentScansEachReturnFullResults(t *testing.T) {
	dir := t.TempDir()
	schema := peopleSchema()
	b1 := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer b1.Release()
	b2 := peopleBatch(t, schema, []int64{3, 4}, []string{"carol", "dave"})
	defer b2.Release()
	path := writeParquetFile(t, dir, "people.parquet", schema, b1, b2)

	p, err := provider.NewTableProvider(context.Background(), path)
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
	dir := t.TempDir()
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()
	path := writeParquetFile(t, dir, "people.parquet", schema, batch)

	p, err := provider.NewTableProvider(context.Background(), path)
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
