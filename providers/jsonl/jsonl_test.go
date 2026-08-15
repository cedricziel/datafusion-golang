package jsonl_test

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
	provider "github.com/cedricziel/datafusion-golang/providers/jsonl"
)

func peopleSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

func writeJSONL(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "data.jsonl")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

// TestNewTableProvider_ValidObjectOnRegisteredBackend covers the
// object-store spec's "Valid object on a registered backend" scenario.
func TestNewTableProvider_ValidObjectOnRegisteredBackend(t *testing.T) {
	location := "mem://" + t.Name() + "/data.jsonl"
	providertest.WriteObject(t, location, "{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"name\":\"bob\"}\n")

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
	path := writeJSONL(t, `{"id":1,"name":"alice"}`+"\n"+`{"id":2,"name":"bob"}`+"\n")
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
	_, err := provider.NewTableProvider(context.Background(), filepath.Join(t.TempDir(), "missing.jsonl"), peopleSchema())
	if err == nil {
		t.Fatalf("expected an error for a missing file")
	}
}

func TestNewTableProvider_FirstObjectSchemaMismatch(t *testing.T) {
	path := writeJSONL(t, `{"id":"not-a-number","name":"alice"}`+"\n")
	_, err := provider.NewTableProvider(context.Background(), path, peopleSchema())
	if err == nil {
		t.Fatalf("expected an error for a first-object schema mismatch")
	}
}

func TestNewTableProvider_EmptyFile(t *testing.T) {
	path := writeJSONL(t, "")
	p, err := provider.NewTableProvider(context.Background(), path, peopleSchema())
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer reader.Release()
	for reader.Next() {
		t.Fatalf("expected no rows from an empty file")
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestScan_MultiObjectCorrectnessInFileOrder(t *testing.T) {
	path := writeJSONL(t, `{"id":1,"name":"alice"}`+"\n"+
		`{"id":2,"name":"bob"}`+"\n"+
		`{"id":3,"name":"carol"}`+"\n"+
		`{"id":4,"name":"dave"}`+"\n")
	p, err := provider.NewTableProvider(context.Background(), path, peopleSchema())
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	ids, names := collectRows(t, reader)
	if len(ids) != 4 {
		t.Fatalf("expected 4 rows, got %v", ids)
	}
	wantNames := []string{"alice", "bob", "carol", "dave"}
	for i, n := range wantNames {
		if names[i] != n {
			t.Fatalf("row %d: expected %q, got %q", i, n, names[i])
		}
	}
}

func TestScan_LaterObjectDecodeFailureSurfacesViaErrWithoutCorruptingEarlierRows(t *testing.T) {
	path := writeJSONL(t, `{"id":1,"name":"alice"}`+"\n"+
		`{"id":"not-a-number","name":"bob"}`+"\n")
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
		t.Fatalf("expected the first (valid) row to be readable")
	}
	rec := reader.RecordBatch()
	idCol := rec.Column(0).(*array.Int64)
	if idCol.Value(0) != 1 {
		t.Fatalf("expected first row id=1, got %d", idCol.Value(0))
	}

	for reader.Next() {
		// drain
	}
	if reader.Err() == nil {
		t.Fatalf("expected a later-object decode failure to surface via Err()")
	}
}

func TestRegisterAndQueryViaSQL(t *testing.T) {
	path := writeJSONL(t, `{"id":1,"name":"alice"}`+"\n"+`{"id":2,"name":"bob"}`+"\n")
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
	path := writeJSONL(t, `{"id":1,"name":"alice"}`+"\n"+`{"id":2,"name":"bob"}`+"\n")
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
	path := writeJSONL(t, `{"id":1,"name":"alice"}`+"\n"+
		`{"id":2,"name":"bob"}`+"\n"+
		`{"id":3,"name":"carol"}`+"\n"+
		`{"id":4,"name":"dave"}`+"\n")
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
