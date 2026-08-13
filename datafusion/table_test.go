package datafusion_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

// memTable is an in-memory TableProvider used across the table tests. It
// counts Scan invocations so tests can assert when the engine did (not)
// reach into Go.
type memTable struct {
	schema    *arrow.Schema
	records   []arrow.RecordBatch
	scanCalls atomic.Int64
}

func (m *memTable) Schema() *arrow.Schema { return m.schema }

func (m *memTable) Scan(ctx context.Context) (array.RecordReader, error) {
	m.scanCalls.Add(1)
	return array.NewRecordReader(m.schema, m.records)
}

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

func newPeopleTable(t *testing.T, batches ...arrow.RecordBatch) *memTable {
	t.Helper()
	m := &memTable{schema: peopleSchema(), records: batches}
	t.Cleanup(func() {
		for _, r := range m.records {
			r.Release()
		}
	})
	return m
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
			// Value returns a zero-copy view into the imported buffer;
			// clone so the string outlives reader.Release().
			names = append(names, strings.Clone(nameCol.Value(i)))
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reading result: %v", err)
	}
	return ids, names
}

func TestRegisterTable_RegisterAndQuery(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	table := newPeopleTable(t, peopleBatch(t, peopleSchema(), []int64{1, 2}, []string{"alice", "bob"}))
	if err := ctx.RegisterTable("people", table); err != nil {
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
	if got := table.scanCalls.Load(); got != 1 {
		t.Fatalf("expected exactly one scan, got %d", got)
	}
}

func TestRegisterTable_DuplicateNameFails(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	first := newPeopleTable(t, peopleBatch(t, peopleSchema(), []int64{1}, []string{"alice"}))
	if err := ctx.RegisterTable("people", first); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}
	second := newPeopleTable(t, peopleBatch(t, peopleSchema(), []int64{9}, []string{"mallory"}))
	err = ctx.RegisterTable("people", second)
	if err == nil {
		t.Fatalf("expected duplicate registration to fail")
	}
	if !strings.Contains(err.Error(), "already") {
		t.Fatalf("expected duplicate-name error, got: %v", err)
	}

	// original table must remain queryable, unchanged
	reader, err := ctx.SQL("SELECT id, name FROM people")
	if err != nil {
		t.Fatalf("SQL after failed duplicate registration: %v", err)
	}
	ids, names := collectRows(t, reader)
	if len(ids) != 1 || ids[0] != 1 || names[0] != "alice" {
		t.Fatalf("original table changed: ids=%v names=%v", ids, names)
	}
}

func TestRegisterTable_UnknownColumnFailsWithoutScan(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	table := newPeopleTable(t, peopleBatch(t, peopleSchema(), []int64{1}, []string{"alice"}))
	if err := ctx.RegisterTable("people", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	_, err = ctx.SQL("SELECT missing_col FROM people")
	if err == nil {
		t.Fatalf("expected planning error for unknown column")
	}
	if !strings.Contains(err.Error(), "missing_col") {
		t.Fatalf("expected error to identify the column, got: %v", err)
	}
	if got := table.scanCalls.Load(); got != 0 {
		t.Fatalf("scan must not run for a query rejected at planning, got %d calls", got)
	}
}

func TestRegisterTable_MultiBatchScan(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	schema := peopleSchema()
	table := newPeopleTable(t,
		peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"}),
		peopleBatch(t, schema, []int64{3, 4}, []string{"carol", "dave"}),
	)
	if err := ctx.RegisterTable("people", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	reader, err := ctx.SQL("SELECT id, name FROM people ORDER BY id")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	ids, _ := collectRows(t, reader)
	if len(ids) != 4 {
		t.Fatalf("expected all 4 rows across both batches, got %v", ids)
	}
	for i, want := range []int64{1, 2, 3, 4} {
		if ids[i] != want {
			t.Fatalf("row %d: expected %d, got %d", i, want, ids[i])
		}
	}
}

func TestRegisterTable_EmptyTable(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	table := newPeopleTable(t) // zero batches
	if err := ctx.RegisterTable("people", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	reader, err := ctx.SQL("SELECT id, name FROM people")
	if err != nil {
		t.Fatalf("SQL on empty table: %v", err)
	}
	defer reader.Release()
	if reader.Schema().NumFields() != 2 {
		t.Fatalf("expected the table schema even for an empty result")
	}
	for reader.Next() {
		if reader.RecordBatch().NumRows() != 0 {
			t.Fatalf("expected no rows from an empty table")
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// errAfterReader yields one good batch, then fails with a fixed error.
type errAfterReader struct {
	schema   *arrow.Schema
	batch    arrow.RecordBatch
	consumed bool
	done     bool
}

func (r *errAfterReader) Retain()  {}
func (r *errAfterReader) Release() {}

func (r *errAfterReader) Schema() *arrow.Schema { return r.schema }

func (r *errAfterReader) Next() bool {
	if r.consumed {
		r.done = true
		return false
	}
	r.consumed = true
	return true
}

func (r *errAfterReader) RecordBatch() arrow.RecordBatch { return r.batch }
func (r *errAfterReader) Record() arrow.RecordBatch      { return r.batch }

func (r *errAfterReader) Err() error {
	if r.done {
		return errors.New("mid-stream scan failure from Go")
	}
	return nil
}

// errTable scans into an errAfterReader.
type errTable struct {
	schema *arrow.Schema
	batch  arrow.RecordBatch
}

func (e *errTable) Schema() *arrow.Schema { return e.schema }

func (e *errTable) Scan(ctx context.Context) (array.RecordReader, error) {
	e.batch.Retain()
	return &errAfterReader{schema: e.schema, batch: e.batch}, nil
}

func TestRegisterTable_ScanErrorMidStreamLeavesSessionUsable(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	if err := ctx.RegisterTable("flaky", &errTable{schema: schema, batch: batch}); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	_, err = ctx.SQL("SELECT * FROM flaky")
	if err == nil {
		t.Fatalf("expected the scan error to abort the query")
	}
	if !strings.Contains(err.Error(), "mid-stream scan failure") {
		t.Fatalf("expected the Go error message to surface, got: %v", err)
	}

	// session must remain usable
	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("SQL after scan error: %v", err)
	}
	reader.Release()
}

// scanErrTable fails at scan-open time.
type scanErrTable struct {
	schema *arrow.Schema
}

func (e *scanErrTable) Schema() *arrow.Schema { return e.schema }

func (e *scanErrTable) Scan(ctx context.Context) (array.RecordReader, error) {
	return nil, errors.New("scan refused by Go")
}

func TestRegisterTable_ScanOpenErrorLeavesSessionUsable(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	if err := ctx.RegisterTable("refusing", &scanErrTable{schema: peopleSchema()}); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}
	_, err = ctx.SQL("SELECT * FROM refusing")
	if err == nil || !strings.Contains(err.Error(), "scan refused by Go") {
		t.Fatalf("expected the Go scan error to surface, got: %v", err)
	}
	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("SQL after scan error: %v", err)
	}
	reader.Release()
}

// panicTable panics during Schema and Scan depending on mode.
type panicTable struct {
	schema *arrow.Schema
}

func (p *panicTable) Schema() *arrow.Schema { return p.schema }

func (p *panicTable) Scan(ctx context.Context) (array.RecordReader, error) {
	panic("scan panicked in Go")
}

func TestRegisterTable_ScanPanicSurfacesAsError(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	if err := ctx.RegisterTable("panicky", &panicTable{schema: peopleSchema()}); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}
	_, err = ctx.SQL("SELECT * FROM panicky")
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("expected the Go panic to surface as an error, got: %v", err)
	}
	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("SQL after panic: %v", err)
	}
	reader.Release()
}

func TestRegisterTable_NotInvokedAfterClose(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}

	table := newPeopleTable(t, peopleBatch(t, peopleSchema(), []int64{1}, []string{"alice"}))
	if err := ctx.RegisterTable("people", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}
	if err := ctx.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := ctx.SQL("SELECT * FROM people"); !errors.Is(err, datafusion.ErrSessionClosed) {
		t.Fatalf("expected ErrSessionClosed, got: %v", err)
	}
	if got := table.scanCalls.Load(); got != 0 {
		t.Fatalf("table must not be scanned after Close, got %d calls", got)
	}

	// registration on a closed session must fail cleanly too
	other := newPeopleTable(t, peopleBatch(t, peopleSchema(), []int64{2}, []string{"bob"}))
	if err := ctx.RegisterTable("other", other); !errors.Is(err, datafusion.ErrSessionClosed) {
		t.Fatalf("expected ErrSessionClosed for registration after Close, got: %v", err)
	}
}

func TestRegisterTable_ConcurrentQueries(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	schema := peopleSchema()
	table := newPeopleTable(t,
		peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"}),
		peopleBatch(t, schema, []int64{3, 4}, []string{"carol", "dave"}),
	)
	if err := ctx.RegisterTable("people", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader, err := ctx.SQL("SELECT id, name FROM people ORDER BY id")
			if err != nil {
				errs <- err
				return
			}
			defer reader.Release()
			var rows int64
			for reader.Next() {
				rows += reader.RecordBatch().NumRows()
			}
			if err := reader.Err(); err != nil {
				errs <- err
				return
			}
			if rows != 4 {
				errs <- fmt.Errorf("expected 4 rows, got %d", rows)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("goroutine error: %v", err)
	}
}
