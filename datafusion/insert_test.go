package datafusion_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

// memInsertTable is an in-memory datafusion.WritableTableProvider.
// Committed rows are visible to Scan immediately after InsertInto
// returns; InsertInto buffers the incoming batches fully before
// committing, matching the "commit or roll back before returning"
// contract.
type memInsertTable struct {
	mu        sync.Mutex
	schema    *arrow.Schema
	batches   []arrow.RecordBatch
	insertOps []datafusion.InsertOp
	insertErr error
	panics    bool
}

func newMemInsertTable(schema *arrow.Schema) *memInsertTable {
	return &memInsertTable{schema: schema}
}

func (m *memInsertTable) Schema() *arrow.Schema { return m.schema }

func (m *memInsertTable) Scan(ctx context.Context) (array.RecordReader, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	batches := make([]arrow.RecordBatch, len(m.batches))
	copy(batches, m.batches)
	for _, b := range batches {
		b.Retain()
	}
	return array.NewRecordReader(m.schema, batches)
}

func (m *memInsertTable) recordedOps() []datafusion.InsertOp {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]datafusion.InsertOp(nil), m.insertOps...)
}

func (m *memInsertTable) InsertInto(ctx context.Context, op datafusion.InsertOp, rows array.RecordReader) (uint64, error) {
	if m.panics {
		panic("insert panicked in Go")
	}

	m.mu.Lock()
	m.insertOps = append(m.insertOps, op)
	insertErr := m.insertErr
	m.mu.Unlock()
	if insertErr != nil {
		return 0, insertErr
	}

	var newBatches []arrow.RecordBatch
	var count uint64
	for rows.Next() {
		rec := rows.RecordBatch()
		rec.Retain()
		newBatches = append(newBatches, rec)
		count += uint64(rec.NumRows())
	}
	if err := rows.Err(); err != nil {
		for _, b := range newBatches {
			b.Release()
		}
		return 0, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	switch op {
	case datafusion.InsertAppend:
		m.batches = append(m.batches, newBatches...)
	case datafusion.InsertOverwrite:
		for _, b := range m.batches {
			b.Release()
		}
		m.batches = newBatches
	default:
		for _, b := range newBatches {
			b.Release()
		}
		return 0, fmt.Errorf("insert op %d not supported by memInsertTable", op)
	}
	return count, nil
}

func countResult(t *testing.T, reader array.RecordReader) uint64 {
	t.Helper()
	defer reader.Release()
	if !reader.Next() {
		t.Fatalf("expected one DML result batch")
	}
	rec := reader.RecordBatch()
	col, ok := rec.Column(0).(*array.Uint64)
	if !ok {
		t.Fatalf("expected count column to be uint64, got %T", rec.Column(0))
	}
	if col.Len() != 1 {
		t.Fatalf("expected a single count row, got %d", col.Len())
	}
	v := col.Value(0)
	if err := reader.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return v
}

func TestInsertInto_ValuesThenSelectSeesRows(t *testing.T) {
	table := newMemInsertTable(peopleSchema())
	ctx := newSession(t)
	if err := ctx.RegisterTable("t", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	reader, err := ctx.SQL("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if got := countResult(t, reader); got != 2 {
		t.Fatalf("expected count=2, got %d", got)
	}

	selectReader, err := ctx.SQL("SELECT id, name FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	ids, names := collectRows(t, selectReader)
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("unexpected ids: %v", ids)
	}
	if names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("unexpected names: %v", names)
	}
	if ops := table.recordedOps(); len(ops) != 1 || ops[0] != datafusion.InsertAppend {
		t.Fatalf("expected one recorded Append, got %v", ops)
	}
}

func TestInsertInto_ColumnListFillsOmittedWithNull(t *testing.T) {
	table := newMemInsertTable(peopleSchema())
	ctx := newSession(t)
	if err := ctx.RegisterTable("t", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	reader, err := ctx.SQL("INSERT INTO t (name) VALUES ('x')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if got := countResult(t, reader); got != 1 {
		t.Fatalf("expected count=1, got %d", got)
	}

	selectReader, err := ctx.SQL("SELECT id, name FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer selectReader.Release()
	if !selectReader.Next() {
		t.Fatalf("expected one row")
	}
	rec := selectReader.RecordBatch()
	idCol := rec.Column(0).(*array.Int64)
	nameCol := rec.Column(1).(*array.String)
	if !idCol.IsNull(0) {
		t.Fatalf("expected the omitted id column to be NULL, got %d", idCol.Value(0))
	}
	if nameCol.Value(0) != "x" {
		t.Fatalf("unexpected name: %q", nameCol.Value(0))
	}
}

func TestInsertInto_SelectFromAnotherProvider(t *testing.T) {
	src := newPeopleTable(t, peopleBatch(t, peopleSchema(), []int64{1, 2}, []string{"alice", "bob"}))
	dst := newMemInsertTable(peopleSchema())

	ctx := newSession(t)
	if err := ctx.RegisterTable("src", src); err != nil {
		t.Fatalf("RegisterTable(src): %v", err)
	}
	if err := ctx.RegisterTable("dst", dst); err != nil {
		t.Fatalf("RegisterTable(dst): %v", err)
	}

	reader, err := ctx.SQL("INSERT INTO dst SELECT * FROM src")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if got := countResult(t, reader); got != 2 {
		t.Fatalf("expected count=2, got %d", got)
	}
}

func TestInsertInto_ZeroRowInsertReportsCountZero(t *testing.T) {
	table := newMemInsertTable(peopleSchema())
	ctx := newSession(t)
	if err := ctx.RegisterTable("t", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	reader, err := ctx.SQL("INSERT INTO t SELECT * FROM t WHERE id > 0")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if got := countResult(t, reader); got != 0 {
		t.Fatalf("expected count=0, got %d", got)
	}
}

func TestInsertInto_NonWritableProviderRejectedWithoutInvocation(t *testing.T) {
	table := newPeopleTable(t)
	ctx := newSession(t)
	if err := ctx.RegisterTable("ro", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	_, err := ctx.SQL("INSERT INTO ro VALUES (1, 'alice')")
	if err == nil {
		t.Fatalf("expected an error inserting into a non-writable provider")
	}
	if !strings.Contains(err.Error(), "does not support") && !strings.Contains(err.Error(), "not implemented") {
		t.Fatalf("expected a clear 'not writable' message, got: %v", err)
	}
	if table.scanCalls.Load() != 0 {
		t.Fatalf("provider must not be invoked for a rejected insert")
	}

	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("session should remain usable: %v", err)
	}
	reader.Release()
}

func TestInsertInto_RejectsUnsupportedMode(t *testing.T) {
	// memInsertTable implements Append and Overwrite but rejects Replace
	// itself (its InsertInto default case) — REPLACE INTO is the mode
	// that actually exercises a provider-side rejection.
	table := newMemInsertTable(peopleSchema())
	ctx := newSession(t)
	if err := ctx.RegisterTable("t", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	_, err := ctx.SQL("REPLACE INTO t VALUES (1, 'alice')")
	if err == nil {
		t.Fatalf("expected an error rejecting a mode memInsertTable rejects")
	}
	if !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("expected the provider's own rejection message, got: %v", err)
	}
}

func TestInsertInto_OverwriteReplacesExistingRows(t *testing.T) {
	table := newMemInsertTable(peopleSchema())
	ctx := newSession(t)
	if err := ctx.RegisterTable("t", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}
	if reader, err := ctx.SQL("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')"); err != nil {
		t.Fatalf("seed INSERT: %v", err)
	} else {
		reader.Release()
	}

	reader, err := ctx.SQL("INSERT OVERWRITE t VALUES (9, 'zed')")
	if err != nil {
		t.Fatalf("INSERT OVERWRITE: %v", err)
	}
	if got := countResult(t, reader); got != 1 {
		t.Fatalf("expected count=1, got %d", got)
	}

	selectReader, err := ctx.SQL("SELECT id, name FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	ids, names := collectRows(t, selectReader)
	if len(ids) != 1 || ids[0] != 9 || names[0] != "zed" {
		t.Fatalf("expected overwrite to replace all rows with (9, zed), got ids=%v names=%v", ids, names)
	}
	ops := table.recordedOps()
	if len(ops) != 2 || ops[1] != datafusion.InsertOverwrite {
		t.Fatalf("expected the second recorded op to be Overwrite, got %v", ops)
	}
}

func TestInsertInto_ProviderErrorSurfacesAndSessionStaysUsable(t *testing.T) {
	table := newMemInsertTable(peopleSchema())
	table.insertErr = errors.New("insert backend unavailable")
	ctx := newSession(t)
	if err := ctx.RegisterTable("t", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	_, err := ctx.SQL("INSERT INTO t VALUES (1, 'alice')")
	if err == nil {
		t.Fatalf("expected the provider's insert error to abort the statement")
	}
	if !strings.Contains(err.Error(), "insert backend unavailable") {
		t.Fatalf("expected the error to describe the failure, got: %v", err)
	}

	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("session should remain usable: %v", err)
	}
	reader.Release()
}

func TestInsertInto_ProviderPanicSurfacesAndSessionStaysUsable(t *testing.T) {
	table := newMemInsertTable(peopleSchema())
	table.panics = true
	ctx := newSession(t)
	if err := ctx.RegisterTable("t", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	_, err := ctx.SQL("INSERT INTO t VALUES (1, 'alice')")
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("expected the Go panic to surface as an error, got: %v", err)
	}

	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("session should remain usable: %v", err)
	}
	reader.Release()
}

func TestInsertInto_NotInvokedAfterClose(t *testing.T) {
	table := newMemInsertTable(peopleSchema())
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	if err := ctx.RegisterTable("t", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}
	if err := ctx.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := ctx.SQL("INSERT INTO t VALUES (1, 'alice')"); !errors.Is(err, datafusion.ErrSessionClosed) {
		t.Fatalf("expected ErrSessionClosed, got: %v", err)
	}
	if len(table.recordedOps()) != 0 {
		t.Fatalf("provider must not be invoked after Close")
	}
}

func TestInsertInto_ConcurrentInsertsAndScans(t *testing.T) {
	table := newMemInsertTable(peopleSchema())
	ctx := newSession(t)
	if err := ctx.RegisterTable("t", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*2)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			sql := fmt.Sprintf("INSERT INTO t VALUES (%d, 'x')", n)
			reader, err := ctx.SQL(sql)
			if err != nil {
				errs <- err
				return
			}
			reader.Release()
		}(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader, err := ctx.SQL("SELECT * FROM t")
			if err != nil {
				errs <- err
				return
			}
			reader.Release()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("goroutine error: %v", err)
		}
	}

	final, err := ctx.SQL("SELECT id FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer final.Release()
	var rows int
	for final.Next() {
		rows += int(final.RecordBatch().NumRows())
	}
	if err := final.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rows != goroutines {
		t.Fatalf("expected %d committed rows, got %d", goroutines, rows)
	}
}
