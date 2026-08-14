package iceberg

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

// writableTableProvider wraps tableProvider for catalog-backed Iceberg
// tables, additionally implementing datafusion.WritableTableProvider
// (design D4 of wire-parquet-and-iceberg-insert). Only
// NewTableProviderFromCatalog returns this type: committing an insert
// requires a catalog, which the metadata-location tableProvider never has.
type writableTableProvider struct {
	*tableProvider
	cat   catalog.Catalog
	ident table.Identifier

	// insertMu serializes InsertInto calls against this instance (design
	// D7): not required for correctness (the catalog arbitrates commits),
	// but it avoids same-instance inserts burning iceberg-go's own
	// commit-retry budget against each other. Scans never take it.
	insertMu sync.Mutex
}

// InsertInto implements datafusion.WritableTableProvider. Append and
// Overwrite both load a fresh table handle via the catalog (design D5) so
// each insert commits against the table's latest state, then delegate to
// Table.Append / Table.Overwrite (with no filter, replacing all existing
// data) — iceberg-go's own commit machinery supplies atomicity and
// conflict retry. Replace is rejected: neither this provider nor a bare
// Iceberg table has key metadata to replace on.
//
// The reported count is the number of rows consumed from rows, matching
// the engine's "provider's word, forwarded verbatim" contract. A zero-row
// Append short-circuits without a commit; a zero-row Overwrite still
// commits the (now empty) replacement.
func (t *writableTableProvider) InsertInto(ctx context.Context, op datafusion.InsertOp, rows array.RecordReader) (uint64, error) {
	if op != datafusion.InsertAppend && op != datafusion.InsertOverwrite {
		rows.Release()
		return 0, fmt.Errorf("iceberg: insert op %d not supported (only Append and Overwrite)", op)
	}

	t.insertMu.Lock()
	defer t.insertMu.Unlock()

	hasFirst := rows.Next()
	if !hasFirst {
		if err := rows.Err(); err != nil {
			rows.Release()
			return 0, fmt.Errorf("iceberg: reading insert input: %w", err)
		}
		if op == datafusion.InsertAppend {
			rows.Release()
			return 0, nil
		}
	}

	var first arrow.RecordBatch
	if hasFirst {
		first = rows.RecordBatch()
		first.Retain()
	}
	// wrapped takes over releasing rows: iceberg-go's Append/Overwrite
	// both `defer rdr.Release()` internally, satisfying InsertInto's
	// "must release rows" contract exactly once.
	wrapped := newFieldIDStrippingReader(rows, t.schema, first)

	tbl, err := t.cat.LoadTable(ctx, t.ident)
	if err != nil {
		wrapped.Release()
		return 0, fmt.Errorf("iceberg: loading %s for insert: %w", t.describe(), err)
	}

	switch op {
	case datafusion.InsertAppend:
		if _, err := tbl.Append(ctx, wrapped, nil); err != nil {
			return 0, fmt.Errorf("iceberg: append to %s: %w", t.describe(), err)
		}
	case datafusion.InsertOverwrite:
		if _, err := tbl.Overwrite(ctx, wrapped, nil); err != nil {
			return 0, fmt.Errorf("iceberg: overwrite %s: %w", t.describe(), err)
		}
	}

	return wrapped.rowsConsumed(), nil
}

// fieldIDStrippingReader wraps an array.RecordReader, presenting every
// batch rebuilt under schema — a zero-copy view sharing the batch's
// column arrays — instead of the batch's own schema (design D6). schema
// is the provider's registered schema, derived with includeFieldIDs=false,
// so batches handed to iceberg-go never carry field-id metadata the
// engine's FFI round-trip may have partially preserved; ArrowSchemaToIceberg
// then deterministically resolves fields by name through the table's own
// mapping instead of risking a partial-id-set conversion error.
//
// If first is non-nil, it is yielded before any batch from inner and then
// released; first must already carry an owned reference (i.e. the caller
// Retain()'d it). Rows are counted as they are consumed, available via
// rowsConsumed after the reader is exhausted.
type fieldIDStrippingReader struct {
	inner  array.RecordReader
	schema *arrow.Schema
	first  arrow.RecordBatch
	cur    arrow.RecordBatch
	rows   uint64
}

func newFieldIDStrippingReader(inner array.RecordReader, schema *arrow.Schema, first arrow.RecordBatch) *fieldIDStrippingReader {
	return &fieldIDStrippingReader{inner: inner, schema: schema, first: first}
}

func (r *fieldIDStrippingReader) Retain() { r.inner.Retain() }

func (r *fieldIDStrippingReader) Release() {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.first != nil {
		r.first.Release()
		r.first = nil
	}
	r.inner.Release()
}

func (r *fieldIDStrippingReader) Schema() *arrow.Schema { return r.schema }
func (r *fieldIDStrippingReader) Err() error            { return r.inner.Err() }

func (r *fieldIDStrippingReader) Next() bool {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}

	var rec arrow.RecordBatch
	switch {
	case r.first != nil:
		rec = r.first
		r.first = nil
		defer rec.Release()
	case r.inner.Next():
		rec = r.inner.RecordBatch()
	default:
		return false
	}

	r.cur = array.NewRecordBatch(r.schema, rec.Columns(), rec.NumRows())
	r.rows += uint64(r.cur.NumRows())
	return true
}

func (r *fieldIDStrippingReader) RecordBatch() arrow.RecordBatch { return r.cur }
func (r *fieldIDStrippingReader) Record() arrow.RecordBatch      { return r.cur }

func (r *fieldIDStrippingReader) rowsConsumed() uint64 { return r.rows }
