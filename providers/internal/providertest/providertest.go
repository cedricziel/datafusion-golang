// Package providertest holds small test doubles and assertions shared by
// providers/parquet and providers/iceberg's InsertInto test suites. It has
// no dependency on either provider package, only arrow-go, so importing it
// adds nothing to either provider's own dependency footprint.
package providertest

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// ErrAfterReader wraps a RecordReader and fails after successfully
// yielding FailAfter batches, so tests can inject a mid-stream failure at
// a specific point without a real broken data source.
type ErrAfterReader struct {
	array.RecordReader
	FailAfter int

	calls int
	err   error
}

func (r *ErrAfterReader) Next() bool {
	if r.calls >= r.FailAfter {
		r.err = errors.New("injected read failure")
		return false
	}
	ok := r.RecordReader.Next()
	if ok {
		r.calls++
	}
	return ok
}

func (r *ErrAfterReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.RecordReader.Err()
}

// NewRowsReader builds a RecordReader over batches, retaining each one on
// the caller's behalf (matching the ownership a real insert stream hands
// to InsertInto).
func NewRowsReader(t *testing.T, schema *arrow.Schema, batches ...arrow.RecordBatch) array.RecordReader {
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

// CountResult reads the single uint64 count row DataFusion returns for a
// DML statement's result.
func CountResult(t *testing.T, reader array.RecordReader) uint64 {
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
