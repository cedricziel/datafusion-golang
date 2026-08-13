package datafusion_test

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

func TestSQL_LiteralQuery(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	defer reader.Release()

	schema := reader.Schema()
	if schema.NumFields() != 1 {
		t.Fatalf("expected 1 field, got %d", schema.NumFields())
	}
	if schema.Field(0).Name != "one" {
		t.Fatalf("expected field named %q, got %q", "one", schema.Field(0).Name)
	}

	if !reader.Next() {
		t.Fatalf("expected at least one batch, err: %v", reader.Err())
	}
	rec := reader.RecordBatch()
	if rec.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", rec.NumRows())
	}
	col, ok := rec.Column(0).(*array.Int64)
	if !ok {
		t.Fatalf("expected column 0 to be int64, got %T", rec.Column(0))
	}
	if col.Value(0) != 1 {
		t.Fatalf("expected value 1, got %d", col.Value(0))
	}

	if reader.Next() {
		t.Fatalf("expected exactly one batch")
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("unexpected error after end of stream: %v", err)
	}
}

func TestSQL_InvalidSQLReturnsErrorAndSessionStaysUsable(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	_, err = ctx.SQL("SELEKT 1")
	if err == nil {
		t.Fatalf("expected an error for invalid SQL")
	}
	if !strings.Contains(strings.ToUpper(err.Error()), "SELEKT") {
		t.Fatalf("expected error message to reference the offending SQL, got: %v", err)
	}

	// session must remain usable for subsequent queries
	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("SQL after invalid SQL: %v", err)
	}
	defer reader.Release()
	if !reader.Next() {
		t.Fatalf("expected a batch from the follow-up query")
	}
}

func TestSQL_UseAfterCloseReturnsError(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	if err := ctx.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err = ctx.SQL("SELECT 1 AS one")
	if !errors.Is(err, datafusion.ErrSessionClosed) {
		t.Fatalf("expected ErrSessionClosed, got: %v", err)
	}

	// Close should be idempotent
	if err := ctx.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestSQL_EmptyResult(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	reader, err := ctx.SQL("SELECT 1 AS one WHERE 1 = 0")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	defer reader.Release()

	if reader.Schema().NumFields() != 1 {
		t.Fatalf("expected schema with 1 field even for an empty result")
	}
	for reader.Next() {
		if reader.RecordBatch().NumRows() != 0 {
			t.Fatalf("expected only empty batches for an empty result")
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSQL_EarlyReleaseIsSafe(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	reader.Release()

	// releasing before full consumption must not corrupt the session
	reader2, err := ctx.SQL("SELECT 2 AS two")
	if err != nil {
		t.Fatalf("SQL after early release: %v", err)
	}
	defer reader2.Release()
	if !reader2.Next() {
		t.Fatalf("expected a batch from the follow-up query")
	}
}

func TestSQL_ConcurrentQueriesOnOneSession(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	const goroutines = 16
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader, err := ctx.SQL("SELECT 1 AS one")
			if err != nil {
				errs <- err
				return
			}
			defer reader.Release()
			if !reader.Next() {
				errs <- errors.New("expected a batch")
				return
			}
			col, ok := reader.RecordBatch().Column(0).(*array.Int64)
			if !ok || col.Value(0) != 1 {
				errs <- errors.New("unexpected result value")
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("goroutine error: %v", err)
	}
}
