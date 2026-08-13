package datafusion_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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

// pushdownTable is an in-memory PushdownTableProvider. It records every
// ScanOptions it receives and can optionally prune batches using the
// pushed filters, truncate at the limit hint, or ignore the projection
// (to exercise the contract-violation path).
type pushdownTable struct {
	memTable
	mu               sync.Mutex
	scans            []*datafusion.ScanOptions
	prune            bool
	truncate         bool
	ignoreProjection bool
	scanErr          error
}

func (p *pushdownTable) recordedScans() []*datafusion.ScanOptions {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*datafusion.ScanOptions(nil), p.scans...)
}

func (p *pushdownTable) ScanWithOptions(ctx context.Context, opts *datafusion.ScanOptions) (array.RecordReader, error) {
	p.mu.Lock()
	p.scans = append(p.scans, opts)
	p.mu.Unlock()
	if p.scanErr != nil {
		return nil, p.scanErr
	}

	batches := p.records
	if p.prune {
		batches = pruneByIDFilters(batches, opts.Filters)
	}
	if p.truncate && opts.Limit >= 0 {
		var kept []arrow.RecordBatch
		var rows int64
		for _, b := range batches {
			if rows >= opts.Limit {
				break
			}
			kept = append(kept, b)
			rows += b.NumRows()
		}
		batches = kept
	}
	reader, err := array.NewRecordReader(p.schema, batches)
	if err != nil {
		return nil, err
	}
	if p.ignoreProjection || opts.Projection == nil {
		return reader, nil
	}
	return datafusion.ProjectReader(reader, opts.Projection), nil
}

// pruneByIDFilters drops batches in which no row can satisfy an `id > n`
// filter — the "may omit only rows that cannot satisfy the filters" side
// of the contract. Other filter shapes are ignored (which is always
// allowed).
func pruneByIDFilters(batches []arrow.RecordBatch, filters []datafusion.Expr) []arrow.RecordBatch {
	threshold := int64(-1 << 62)
	for _, f := range filters {
		cmp, ok := f.(datafusion.Compare)
		if !ok || cmp.Column.Name != "id" || cmp.Op != datafusion.CompareGt {
			continue
		}
		if v, ok := cmp.Literal.Value.(int64); ok && v > threshold {
			threshold = v
		}
	}
	var kept []arrow.RecordBatch
	for _, b := range batches {
		ids := b.Column(0).(*array.Int64)
		anyMatch := false
		for i := 0; i < ids.Len(); i++ {
			if ids.Value(i) > threshold {
				anyMatch = true
				break
			}
		}
		if anyMatch {
			kept = append(kept, b)
		}
	}
	return kept
}

func newPushdownTable(t *testing.T, batches ...arrow.RecordBatch) *pushdownTable {
	t.Helper()
	p := &pushdownTable{memTable: memTable{schema: peopleSchema(), records: batches}}
	t.Cleanup(func() {
		for _, r := range p.records {
			r.Release()
		}
	})
	return p
}

func newSessionWithPushdownTable(t *testing.T, table *pushdownTable) *datafusion.SessionContext {
	t.Helper()
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	t.Cleanup(func() { ctx.Close() })
	if err := ctx.RegisterTable("people", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}
	return ctx
}

func fourPeopleBatches(t *testing.T) []arrow.RecordBatch {
	t.Helper()
	schema := peopleSchema()
	return []arrow.RecordBatch{
		peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"}),
		peopleBatch(t, schema, []int64{3, 4}, []string{"carol", "dave"}),
	}
}

func collectNames(t *testing.T, reader array.RecordReader) []string {
	t.Helper()
	defer reader.Release()
	var names []string
	for reader.Next() {
		rec := reader.RecordBatch()
		col := rec.Column(0).(*array.String)
		for i := 0; i < col.Len(); i++ {
			names = append(names, strings.Clone(col.Value(i)))
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reading result: %v", err)
	}
	return names
}

func TestPushdown_ProjectionAndFilterDelivered(t *testing.T) {
	table := newPushdownTable(t, fourPeopleBatches(t)...)
	ctx := newSessionWithPushdownTable(t, table)

	reader, err := ctx.SQL("SELECT name FROM people WHERE id > 2 ORDER BY name")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	names := collectNames(t, reader)
	if len(names) != 2 || names[0] != "carol" || names[1] != "dave" {
		t.Fatalf("unexpected names: %v", names)
	}

	scans := table.recordedScans()
	if len(scans) != 1 {
		t.Fatalf("expected exactly one scan, got %d", len(scans))
	}
	opts := scans[0]
	// The engine re-applies the (advisory) filter above the scan, so the
	// projection must include id for the re-filter plus the selected name.
	wantProj := []int{0, 1}
	if !reflect.DeepEqual(opts.Projection, wantProj) {
		t.Fatalf("unexpected projection: %v (want %v)", opts.Projection, wantProj)
	}
	wantFilter := datafusion.Compare{
		Column:  datafusion.Column{Name: "id", Index: 0},
		Op:      datafusion.CompareGt,
		Literal: datafusion.Literal{Type: datafusion.LiteralInt64, Value: int64(2)},
	}
	if len(opts.Filters) != 1 || !reflect.DeepEqual(opts.Filters[0], wantFilter) {
		t.Fatalf("unexpected filters: %#v", opts.Filters)
	}
	if opts.Limit != -1 {
		t.Fatalf("expected no limit hint, got %d", opts.Limit)
	}
}

func TestPushdown_IgnoringFiltersMatchesNonPushdown(t *testing.T) {
	const query = "SELECT id, name FROM people WHERE id > 2 AND name <> 'dave' ORDER BY id"

	pushdown := newPushdownTable(t, fourPeopleBatches(t)...) // ignores filters
	pctx := newSessionWithPushdownTable(t, pushdown)
	reader, err := pctx.SQL(query)
	if err != nil {
		t.Fatalf("SQL (pushdown): %v", err)
	}
	gotIDs, gotNames := collectRows(t, reader)

	plain := newPeopleTable(t, fourPeopleBatches(t)...)
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()
	if err := ctx.RegisterTable("people", plain); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}
	reader, err = ctx.SQL(query)
	if err != nil {
		t.Fatalf("SQL (plain): %v", err)
	}
	wantIDs, wantNames := collectRows(t, reader)

	if !reflect.DeepEqual(gotIDs, wantIDs) || !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("pushdown result (%v, %v) differs from non-pushdown result (%v, %v)",
			gotIDs, gotNames, wantIDs, wantNames)
	}
	if len(pushdown.recordedScans()) != 1 {
		t.Fatalf("expected one pushdown scan")
	}
}

func TestPushdown_PruningProviderReturnsCorrectRows(t *testing.T) {
	table := newPushdownTable(t, fourPeopleBatches(t)...)
	table.prune = true
	ctx := newSessionWithPushdownTable(t, table)

	reader, err := ctx.SQL("SELECT id, name FROM people WHERE id > 2 ORDER BY id")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	ids, names := collectRows(t, reader)
	if !reflect.DeepEqual(ids, []int64{3, 4}) || !reflect.DeepEqual(names, []string{"carol", "dave"}) {
		t.Fatalf("pruned scan returned wrong rows: %v %v", ids, names)
	}
}

func TestPushdown_LimitHint(t *testing.T) {
	for _, truncate := range []bool{false, true} {
		table := newPushdownTable(t, fourPeopleBatches(t)...)
		table.truncate = truncate
		ctx := newSessionWithPushdownTable(t, table)

		reader, err := ctx.SQL("SELECT * FROM people LIMIT 3")
		if err != nil {
			t.Fatalf("SQL (truncate=%v): %v", truncate, err)
		}
		ids, _ := collectRows(t, reader)
		if len(ids) != 3 {
			t.Fatalf("truncate=%v: expected exactly 3 rows, got %v", truncate, ids)
		}

		scans := table.recordedScans()
		if len(scans) != 1 || scans[0].Limit != 3 {
			t.Fatalf("truncate=%v: expected a limit hint of 3, got %+v", truncate, scans)
		}
	}
}

func TestPushdown_SelectStarCoversAllColumns(t *testing.T) {
	table := newPushdownTable(t, fourPeopleBatches(t)...)
	ctx := newSessionWithPushdownTable(t, table)

	reader, err := ctx.SQL("SELECT * FROM people ORDER BY id")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	ids, names := collectRows(t, reader)
	if !reflect.DeepEqual(ids, []int64{1, 2, 3, 4}) {
		t.Fatalf("unexpected ids: %v", ids)
	}
	if len(names) != 4 {
		t.Fatalf("unexpected names: %v", names)
	}

	scans := table.recordedScans()
	if len(scans) != 1 {
		t.Fatalf("expected one scan, got %d", len(scans))
	}
	if proj := scans[0].Projection; proj != nil && !reflect.DeepEqual(proj, []int{0, 1}) {
		t.Fatalf("SELECT * projection must cover every column, got %v", proj)
	}
	if len(scans[0].Filters) != 0 {
		t.Fatalf("expected no filters, got %#v", scans[0].Filters)
	}
	if scans[0].Limit != -1 {
		t.Fatalf("expected no limit hint, got %d", scans[0].Limit)
	}
}

func TestPushdown_ProjectionViolationFailsQuery(t *testing.T) {
	table := newPushdownTable(t, fourPeopleBatches(t)...)
	table.ignoreProjection = true
	ctx := newSessionWithPushdownTable(t, table)

	_, err := ctx.SQL("SELECT name FROM people")
	if err == nil {
		t.Fatalf("expected the projection violation to fail the query")
	}
	if !strings.Contains(err.Error(), "projection contract") {
		t.Fatalf("expected a schema-mismatch error, got: %v", err)
	}

	// session must remain usable
	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("SQL after projection violation: %v", err)
	}
	reader.Release()
}

func TestPushdown_ScanErrorSurfaces(t *testing.T) {
	table := newPushdownTable(t, fourPeopleBatches(t)...)
	table.scanErr = errors.New("pushdown scan refused by Go")
	ctx := newSessionWithPushdownTable(t, table)

	_, err := ctx.SQL("SELECT * FROM people")
	if err == nil || !strings.Contains(err.Error(), "pushdown scan refused by Go") {
		t.Fatalf("expected the Go scan error to surface, got: %v", err)
	}
	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("SQL after scan error: %v", err)
	}
	reader.Release()
}

func TestPushdown_ConcurrentQueries(t *testing.T) {
	table := newPushdownTable(t, fourPeopleBatches(t)...)
	ctx := newSessionWithPushdownTable(t, table)

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader, err := ctx.SQL("SELECT name FROM people WHERE id > 2 ORDER BY name")
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
			if rows != 2 {
				errs <- fmt.Errorf("expected 2 rows, got %d", rows)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("goroutine error: %v", err)
	}
}

func TestProjectReader(t *testing.T) {
	schema := peopleSchema()
	batches := []arrow.RecordBatch{
		peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"}),
		peopleBatch(t, schema, []int64{3}, []string{"carol"}),
	}
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()

	inner, err := array.NewRecordReader(schema, batches)
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	// reorder: name first, id second
	reader := datafusion.ProjectReader(inner, []int{1, 0})
	if got := reader.Schema().NumFields(); got != 2 {
		t.Fatalf("expected 2 fields, got %d", got)
	}
	if reader.Schema().Field(0).Name != "name" || reader.Schema().Field(1).Name != "id" {
		t.Fatalf("unexpected projected schema: %v", reader.Schema())
	}
	var names []string
	var ids []int64
	for reader.Next() {
		rec := reader.RecordBatch()
		nameCol := rec.Column(0).(*array.String)
		idCol := rec.Column(1).(*array.Int64)
		for i := 0; i < int(rec.NumRows()); i++ {
			names = append(names, strings.Clone(nameCol.Value(i)))
			ids = append(ids, idCol.Value(i))
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader error: %v", err)
	}
	reader.Release()
	if !reflect.DeepEqual(names, []string{"alice", "bob", "carol"}) || !reflect.DeepEqual(ids, []int64{1, 2, 3}) {
		t.Fatalf("unexpected projected data: %v %v", names, ids)
	}
}

func TestProjectReader_SingleColumn(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()

	inner, err := array.NewRecordReader(schema, []arrow.RecordBatch{batch})
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	reader := datafusion.ProjectReader(inner, []int{1})
	names := collectNames(t, reader)
	if !reflect.DeepEqual(names, []string{"alice", "bob"}) {
		t.Fatalf("unexpected names: %v", names)
	}
}

func TestProjectReader_OutOfRangePanics(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	inner, err := array.NewRecordReader(schema, []arrow.RecordBatch{batch})
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	defer inner.Release()
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic for out-of-range index")
		}
	}()
	datafusion.ProjectReader(inner, []int{2})
}

func TestPushdown_CountStarEmptyProjection(t *testing.T) {
	table := newPushdownTable(t, fourPeopleBatches(t)...)
	ctx := newSessionWithPushdownTable(t, table)

	reader, err := ctx.SQL("SELECT count(*) AS n FROM people")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	defer reader.Release()
	var count int64 = -1
	for reader.Next() {
		rec := reader.RecordBatch()
		if rec.NumRows() > 0 {
			count = rec.Column(0).(*array.Int64).Value(0)
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reading result: %v", err)
	}
	if count != 4 {
		t.Fatalf("expected count 4, got %d", count)
	}

	scans := table.recordedScans()
	if len(scans) != 1 {
		t.Fatalf("expected one scan, got %d", len(scans))
	}
	if proj := scans[0].Projection; proj != nil && len(proj) != 0 {
		t.Fatalf("count(*) should need zero (or all) columns, got projection %v", proj)
	}
}
