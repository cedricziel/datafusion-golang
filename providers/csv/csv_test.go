package csv_test

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
	provider "github.com/cedricziel/datafusion-golang/providers/csv"
	"github.com/cedricziel/datafusion-golang/providers/internal/providertest"
)

func peopleSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

func writeCSV(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

// TestNewTableProvider_ValidObjectOnRegisteredBackend covers the
// object-store spec's "Valid object on a registered backend" scenario.
func TestNewTableProvider_ValidObjectOnRegisteredBackend(t *testing.T) {
	location := "mem://" + t.Name() + "/data.csv"
	providertest.WriteObject(t, location, "id,name\n1,alice\n2,bob\n")

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
	path := writeCSV(t, "id,name\n1,alice\n2,bob\n")
	p, err := provider.NewTableProvider(context.Background(), path, peopleSchema())
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	if got, want := p.Schema().NumFields(), 2; got != want {
		t.Fatalf("expected %d fields, got %d", want, got)
	}
	if p.Schema().Field(0).Name != "id" || p.Schema().Field(1).Name != "name" {
		t.Fatalf("unexpected field names: %v", p.Schema())
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

func TestNewTableProvider_FieldNamesComeFromSchemaNotHeaderText(t *testing.T) {
	// Spec: "construction succeeds and the resulting provider's schema is
	// exactly the supplied schema" — the header row is used only to
	// validate column count, never to rename fields, even though
	// arrow-go's own csv.Reader (WithHeader(true)) renames fields to the
	// header's text by default.
	path := writeCSV(t, "ID,Name\n1,alice\n2,bob\n")
	p, err := provider.NewTableProvider(context.Background(), path, peopleSchema())
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	if p.Schema().Field(0).Name != "id" || p.Schema().Field(1).Name != "name" {
		t.Fatalf("expected schema field names to stay as supplied (id, name), got: %v", p.Schema())
	}

	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if reader.Schema().Field(0).Name != "id" || reader.Schema().Field(1).Name != "name" {
		reader.Release()
		t.Fatalf("expected scan output field names to match the declared schema (id, name), got: %v", reader.Schema())
	}
	ids, names := collectRows(t, reader)
	if len(ids) != 2 || names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("unexpected data: ids=%v names=%v", ids, names)
	}
}

func TestNewTableProvider_MissingFile(t *testing.T) {
	_, err := provider.NewTableProvider(context.Background(), filepath.Join(t.TempDir(), "missing.csv"), peopleSchema())
	if err == nil {
		t.Fatalf("expected an error for a missing file")
	}
}

func TestNewTableProvider_ColumnCountMismatch(t *testing.T) {
	path := writeCSV(t, "id,name,extra\n1,alice,x\n")
	_, err := provider.NewTableProvider(context.Background(), path, peopleSchema())
	if err == nil {
		t.Fatalf("expected an error for a column count mismatch")
	}
}

func TestNewTableProvider_NoHeaderRowAtAllFails(t *testing.T) {
	path := writeCSV(t, "")
	if _, err := provider.NewTableProvider(context.Background(), path, peopleSchema()); err == nil {
		t.Fatalf("expected an error for a file with no header row")
	}
	if _, err := provider.NewTableProviderWithInferredSchema(context.Background(), path); err == nil {
		t.Fatalf("expected an error for a file with no header row")
	}
}

func TestNewTableProvider_EmptyDataFile(t *testing.T) {
	path := writeCSV(t, "id,name\n")
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
		t.Fatalf("expected no rows from an empty data file")
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewTableProviderWithInferredSchema_ValidFile(t *testing.T) {
	path := writeCSV(t, "id,name\n1,alice\n2,bob\n")
	p, err := provider.NewTableProviderWithInferredSchema(context.Background(), path)
	if err != nil {
		t.Fatalf("NewTableProviderWithInferredSchema: %v", err)
	}
	if got, want := p.Schema().NumFields(), 2; got != want {
		t.Fatalf("expected %d fields, got %d", want, got)
	}
	if p.Schema().Field(0).Name != "id" || p.Schema().Field(1).Name != "name" {
		t.Fatalf("unexpected field names: %v", p.Schema())
	}
	if p.Schema().Field(0).Type.ID() != arrow.INT64 {
		t.Fatalf("expected id column to be inferred as int64, got %v", p.Schema().Field(0).Type)
	}

	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer reader.Release()
	var rows int
	for reader.Next() {
		rows += int(reader.RecordBatch().NumRows())
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rows != 2 {
		t.Fatalf("expected 2 rows, got %d", rows)
	}
}

func TestNewTableProviderWithInferredSchema_HeaderOnlyFails(t *testing.T) {
	path := writeCSV(t, "id,name\n")
	_, err := provider.NewTableProviderWithInferredSchema(context.Background(), path)
	if err == nil {
		t.Fatalf("expected an error: no data rows to infer types from")
	}
}

func TestNewTableProviderWithInferredSchema_LaterRowTypeMismatchSurfacesViaErr(t *testing.T) {
	// id looks like int64 from row 1, but row 2's value doesn't parse as
	// int64 under the type already inferred — arrow-go documents this as
	// producing a null field and ending the scan on the following Next(),
	// with the failure exposed only via Err() (design D3).
	path := writeCSV(t, "id,name\n1,alice\nnotanumber,bob\n3,carol\n")
	p, err := provider.NewTableProviderWithInferredSchema(context.Background(), path)
	if err != nil {
		t.Fatalf("NewTableProviderWithInferredSchema: %v", err)
	}
	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer reader.Release()
	for reader.Next() {
		// drain
	}
	if reader.Err() == nil {
		t.Fatalf("expected a later-row type mismatch to surface via Err()")
	}
}

func TestScan_MultiRowCorrectness(t *testing.T) {
	path := writeCSV(t, "id,name\n1,alice\n2,bob\n3,carol\n4,dave\n")
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

func TestRegisterAndQueryViaSQL(t *testing.T) {
	path := writeCSV(t, "id,name\n1,alice\n2,bob\n")
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
	path := writeCSV(t, "id,name\n1,alice\n2,bob\n")
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
	path := writeCSV(t, "id,name\n1,alice\n2,bob\n3,carol\n4,dave\n")
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
