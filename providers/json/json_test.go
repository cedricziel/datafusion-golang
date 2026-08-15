package json_test

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
	"github.com/cedricziel/datafusion-golang/datafusion"
	"github.com/cedricziel/datafusion-golang/providers/internal/providertest"
	provider "github.com/cedricziel/datafusion-golang/providers/json"
)

func peopleSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

func writeJSON(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

// TestNewTableProvider_ValidObjectOnRegisteredBackend covers the
// object-store spec's "Valid object on a registered backend" scenario.
func TestNewTableProvider_ValidObjectOnRegisteredBackend(t *testing.T) {
	location := "mem://" + t.Name() + "/data.json"
	providertest.WriteObject(t, location, `[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]`)

	p, err := provider.NewTableProvider(context.Background(), location, peopleSchema())
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
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 || names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("got ids=%v names=%v, want [1 2] [alice bob]", ids, names)
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

func TestNewTableProvider_ValidFile(t *testing.T) {
	path := writeJSON(t, `[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]`)
	p, err := provider.NewTableProvider(context.Background(), path, peopleSchema())
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	if got, want := p.Schema().NumFields(), 2; got != want {
		t.Fatalf("expected %d fields, got %d", want, got)
	}

	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	ids, names := collectRows(t, reader)
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("unexpected ids: %v", ids)
	}
	if names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("unexpected names: %v", names)
	}
}

func TestNewTableProvider_MissingFile(t *testing.T) {
	_, err := provider.NewTableProvider(context.Background(), filepath.Join(t.TempDir(), "missing.json"), peopleSchema())
	if err == nil {
		t.Fatalf("expected an error for a missing file")
	}
}

func TestNewTableProvider_TopLevelNotArray(t *testing.T) {
	path := writeJSON(t, `{"id":1,"name":"alice"}`)
	_, err := provider.NewTableProvider(context.Background(), path, peopleSchema())
	if err == nil {
		t.Fatalf("expected an error when the top level is not a JSON array")
	}
}

func TestNewTableProvider_ContentSchemaMismatch(t *testing.T) {
	path := writeJSON(t, `[{"id":"not-a-number","name":"alice"}]`)
	_, err := provider.NewTableProvider(context.Background(), path, peopleSchema())
	if err == nil {
		t.Fatalf("expected an error for a content/schema mismatch")
	}
}

func TestNewTableProvider_EmptyArray(t *testing.T) {
	// RecordFromJSON always produces exactly one record batch (possibly
	// zero rows), unlike providers/csv and providers/jsonl's streaming
	// readers, which return zero batches for empty input — so the
	// contract here is "zero total rows," not "zero batches."
	path := writeJSON(t, `[]`)
	p, err := provider.NewTableProvider(context.Background(), path, peopleSchema())
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	var rows int64
	for reader.Next() {
		rows += reader.RecordBatch().NumRows()
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	reader.Release()
	if rows != 0 {
		t.Fatalf("expected 0 rows from an empty array, got %d", rows)
	}
}

func TestScan_MultiElementCorrectnessInFileOrderAsSingleBatch(t *testing.T) {
	path := writeJSON(t, `[{"id":1,"name":"alice"},{"id":2,"name":"bob"},{"id":3,"name":"carol"},{"id":4,"name":"dave"}]`)
	p, err := provider.NewTableProvider(context.Background(), path, peopleSchema())
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatalf("expected at least one batch")
	}
	rec := reader.RecordBatch()
	if rec.NumRows() != 4 {
		t.Fatalf("expected the whole file to decode as a single 4-row batch, got %d rows", rec.NumRows())
	}
	nameCol := rec.Column(1).(*array.String)
	wantNames := []string{"alice", "bob", "carol", "dave"}
	for i, n := range wantNames {
		if nameCol.Value(i) != n {
			t.Fatalf("row %d: expected %q, got %q", i, n, nameCol.Value(i))
		}
	}
	if reader.Next() {
		t.Fatalf("expected exactly one batch for the whole-file decode")
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRegisterAndQueryViaSQL(t *testing.T) {
	path := writeJSON(t, `[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]`)
	p, err := provider.NewTableProvider(context.Background(), path, peopleSchema())
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

func TestScan_RepeatedScansDoNotLeakFileDescriptors(t *testing.T) {
	path := writeJSON(t, `[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]`)
	p, err := provider.NewTableProvider(context.Background(), path, peopleSchema())
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
	path := writeJSON(t, `[{"id":1,"name":"alice"},{"id":2,"name":"bob"},{"id":3,"name":"carol"},{"id":4,"name":"dave"}]`)
	p, err := provider.NewTableProvider(context.Background(), path, peopleSchema())
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
