package parquet_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cedricziel/datafusion-golang/datafusion"
	provider "github.com/cedricziel/datafusion-golang/providers/parquet"
)

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

func fileHash(t *testing.T, path string) [32]byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile %s: %v", path, err)
	}
	return sha256.Sum256(data)
}

func noLeftoverTempFiles(t *testing.T, dir, base string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", dir, err)
	}
	for _, e := range entries {
		if e.Name() != base {
			t.Fatalf("unexpected leftover file in %s: %s", dir, e.Name())
		}
	}
}

func newWritableProvider(t *testing.T, path string) interface {
	datafusion.TableProvider
	datafusion.WritableTableProvider
} {
	t.Helper()
	p, err := provider.NewTableProvider(path)
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	writable, ok := p.(interface {
		datafusion.TableProvider
		datafusion.WritableTableProvider
	})
	if !ok {
		t.Fatalf("parquet provider does not implement WritableTableProvider")
	}
	return writable
}

func TestInsertInto_RejectsReplace(t *testing.T) {
	dir := t.TempDir()
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	path := writeParquetFile(t, dir, "people.parquet", schema, batch)
	before := fileHash(t, path)

	p := newWritableProvider(t, path)
	rows := newRowsReader(t, schema, peopleBatch(t, schema, []int64{2}, []string{"bob"}))
	_, err := p.InsertInto(context.Background(), datafusion.InsertReplace, rows)
	if err == nil {
		t.Fatalf("expected Replace to be rejected")
	}
	if fileHash(t, path) != before {
		t.Fatalf("file must be untouched by a rejected insert")
	}
}

func TestInsertInto_FailingReaderLeavesOriginalUnchanged(t *testing.T) {
	for _, failAfter := range []int{0, 1, 2} {
		t.Run("", func(t *testing.T) {
			dir := t.TempDir()
			schema := peopleSchema()
			batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
			defer batch.Release()
			path := writeParquetFile(t, dir, "people.parquet", schema, batch)
			before := fileHash(t, path)

			p := newWritableProvider(t, path)
			b1 := peopleBatch(t, schema, []int64{2}, []string{"bob"})
			defer b1.Release()
			b2 := peopleBatch(t, schema, []int64{3}, []string{"carol"})
			defer b2.Release()
			b3 := peopleBatch(t, schema, []int64{4}, []string{"dave"})
			defer b3.Release()
			inner := newRowsReader(t, schema, b1, b2, b3)
			rows := &errAfterReader{RecordReader: inner, failAfter: failAfter}

			_, err := p.InsertInto(context.Background(), datafusion.InsertAppend, rows)
			if err == nil {
				t.Fatalf("expected the injected read failure to abort the insert")
			}
			if fileHash(t, path) != before {
				t.Fatalf("original file must be byte-identical after a failed insert (failAfter=%d)", failAfter)
			}
			noLeftoverTempFiles(t, dir, "people.parquet")
		})
	}
}

func TestInsertInto_ZeroRowAppendLeavesFileUntouched(t *testing.T) {
	dir := t.TempDir()
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	path := writeParquetFile(t, dir, "people.parquet", schema, batch)
	before := fileHash(t, path)

	p := newWritableProvider(t, path)
	rows := newRowsReader(t, schema)
	count, err := p.InsertInto(context.Background(), datafusion.InsertAppend, rows)
	if err != nil {
		t.Fatalf("InsertInto: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected count=0, got %d", count)
	}
	if fileHash(t, path) != before {
		t.Fatalf("zero-row Append must not touch the file")
	}
	noLeftoverTempFiles(t, dir, "people.parquet")
}

func TestInsertInto_ZeroRowOverwriteProducesEmptyFile(t *testing.T) {
	dir := t.TempDir()
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	path := writeParquetFile(t, dir, "people.parquet", schema, batch)

	p := newWritableProvider(t, path)
	rows := newRowsReader(t, schema)
	count, err := p.InsertInto(context.Background(), datafusion.InsertOverwrite, rows)
	if err != nil {
		t.Fatalf("InsertInto: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected count=0, got %d", count)
	}

	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer reader.Release()
	for reader.Next() {
		t.Fatalf("expected zero rows after zero-row Overwrite")
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	noLeftoverTempFiles(t, dir, "people.parquet")
}

func TestInsertInto_SchemaRoundTrips(t *testing.T) {
	dir := t.TempDir()
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	path := writeParquetFile(t, dir, "people.parquet", schema, batch)

	p := newWritableProvider(t, path)
	// Reading any Parquet file back stamps a PARQUET:field_id onto each
	// field, so the schema to round-trip against is p.Schema() (as read
	// back before the insert), not the plain schema used to build rows.
	want := p.Schema()

	rows := newRowsReader(t, schema, peopleBatch(t, schema, []int64{2}, []string{"bob"}))
	if _, err := p.InsertInto(context.Background(), datafusion.InsertAppend, rows); err != nil {
		t.Fatalf("InsertInto: %v", err)
	}

	fresh, err := provider.NewTableProvider(path)
	if err != nil {
		t.Fatalf("NewTableProvider after insert: %v", err)
	}
	if !fresh.Schema().Equal(want) {
		t.Fatalf("schema drifted after rewrite: got %v, want %v", fresh.Schema(), want)
	}
}

func TestInsertInto_AppendThenScanReturnsCombinedRows(t *testing.T) {
	dir := t.TempDir()
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	path := writeParquetFile(t, dir, "people.parquet", schema, batch)

	p := newWritableProvider(t, path)
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
	dir := t.TempDir()
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	path := writeParquetFile(t, dir, "people.parquet", schema, batch)

	p := newWritableProvider(t, path)
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

func TestInsertInto_ConcurrentAppendsBothLand(t *testing.T) {
	dir := t.TempDir()
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	path := writeParquetFile(t, dir, "people.parquet", schema, batch)

	p := newWritableProvider(t, path)
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
		t.Fatalf("expected 3 rows (1 original + 2 concurrent appends), got %d: %v", len(ids), ids)
	}
}

func TestInsertInto_ScanOpenedBeforeInsertReadsPreInsertContents(t *testing.T) {
	dir := t.TempDir()
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	path := writeParquetFile(t, dir, "people.parquet", schema, batch)

	p := newWritableProvider(t, path)
	scan, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	rows := newRowsReader(t, schema, peopleBatch(t, schema, []int64{2}, []string{"bob"}))
	if _, err := p.InsertInto(context.Background(), datafusion.InsertAppend, rows); err != nil {
		t.Fatalf("InsertInto: %v", err)
	}

	ids, names := collectRows(t, scan)
	if len(ids) != 1 || ids[0] != 1 || names[0] != "alice" {
		t.Fatalf("scan opened before insert must see pre-insert contents only, got ids=%v names=%v", ids, names)
	}
}

// TestInsertInto_PushdownAgreesWithFullScanAfterRewrite guards design D3's
// statistics claim: a file produced by InsertInto's rewrite must carry
// valid per-row-group stats and bloom filters, so pushdown-enabled queries
// against it keep agreeing with an unpruned full scan exactly like they do
// for a file that was never rewritten (see TestPushdown_DifferentialBattery
// in pushdown_test.go, which this reuses fixtures and helpers from).
func TestInsertInto_PushdownAgreesWithFullScanAfterRewrite(t *testing.T) {
	path := writeDiffFixture(t)
	schema := diffSchema()

	p := newWritableProvider(t, path)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	for i := range 10 {
		id := int64(30 + i + 1)
		b.Field(0).(*array.Int64Builder).Append(id)
		if id%2 == 0 {
			b.Field(1).(*array.StringBuilder).Append("even-inserted")
		} else {
			b.Field(1).(*array.StringBuilder).Append("odd-inserted")
		}
		b.Field(2).(*array.Float64Builder).Append(float64(id) / 2)
	}
	rec := b.NewRecordBatch()
	b.Release()
	rows := newRowsReader(t, schema, rec)
	rec.Release()

	if _, err := p.InsertInto(context.Background(), datafusion.InsertAppend, rows); err != nil {
		t.Fatalf("InsertInto: %v", err)
	}

	push, full := newDiffSessions(t, path)
	queries := []string{
		"SELECT * FROM t",
		"SELECT * FROM t WHERE id > 15",
		"SELECT * FROM t WHERE id > 35",
		"SELECT * FROM t WHERE id BETWEEN 25 AND 33",
		"SELECT * FROM t WHERE name = 'even-inserted'",
		"SELECT * FROM t WHERE val IS NULL",
		"SELECT COUNT(*) FROM t",
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
