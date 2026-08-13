package datafusion

/*
#include <stdlib.h>
#include <stdint.h>
#include "datafusion_go.h"
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"runtime/cgo"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

// TableProvider is a Go-implemented data source that can be registered on
// a SessionContext and queried via SQL like any built-in table.
//
// Implementations must be safe for concurrent use by multiple goroutines:
// the engine may invoke Schema and Scan from multiple engine-internal
// threads at once when concurrent queries reference the same table
// (exactly like an http.Handler serving concurrent requests).
//
// Scan is invoked once per query execution and must return a fresh
// RecordReader producing the table's rows; the engine consumes it batch
// by batch and releases it when the query completes or fails. The
// returned reader must terminate: there is no cancellation wiring yet, so
// a reader that blocks forever hangs its query indefinitely. Ownership of
// the reader passes to the engine; do not use or release it after
// returning.
type TableProvider interface {
	// Schema returns the table's schema. It is fetched exactly once, at
	// registration time, and cached by the engine.
	Schema() *arrow.Schema
	// Scan starts a full-table scan. The provided context carries no
	// cancellation yet; it is reserved for a future change.
	Scan(ctx context.Context) (array.RecordReader, error)
}

// ScanOptions carries the pushdown information for one scan of a
// PushdownTableProvider.
//
// Projection is the ordered list of column indices (into the registered
// schema) the query requires. nil means all columns; an empty non-nil
// slice means zero columns (the query only needs row counts). The
// provider MUST return exactly the projected columns, in that order — the
// engine validates the returned schema and fails the query on a mismatch.
// Use ProjectReader to satisfy this from a full-width reader.
//
// Filters are the query's pushable predicates, combined with implicit
// AND. They are strictly advisory: the engine re-applies every pushed
// filter to the scan's output, so ignoring them (entirely or partially)
// never changes query results. A provider may use them only to omit rows
// that cannot satisfy the filters; it MUST NOT omit rows that match.
//
// Limit is an advisory fetch hint: the provider may stop after producing
// at least that many rows, or ignore it; the engine enforces the query's
// limit regardless. -1 means no hint. The engine can only push a limit
// past its own re-applied filters, so whenever a query's WHERE clause is
// pushed down, Limit is -1.
type ScanOptions struct {
	Projection []int
	Filters    []Expr
	Limit      int64
}

// PushdownTableProvider is an optional extension of TableProvider. A
// provider that implements it is registered with scan pushdown enabled:
// the engine calls ScanWithOptions instead of Scan for every query,
// passing the projected column set, the pushable filters, and the limit
// hint. Providers implementing only TableProvider keep full-scan
// behavior, unchanged.
type PushdownTableProvider interface {
	TableProvider
	// ScanWithOptions starts a scan honoring opts.Projection exactly and
	// optionally using opts.Filters/opts.Limit to skip data. The returned
	// reader follows the same contract as TableProvider.Scan.
	ScanWithOptions(ctx context.Context, opts *ScanOptions) (array.RecordReader, error)
}

// RegisterTable registers a Go-implemented table under the given name,
// making it queryable via SQL on this session. Registering a name that is
// already in use returns an error and leaves the existing table
// unchanged. The engine holds a reference to the provider until the
// session is closed.
//
// If the provider also implements PushdownTableProvider, it is registered
// with scan pushdown enabled; the capability is detected here and needs
// no separate configuration.
func (s *SessionContext) RegisterTable(name string, provider TableProvider) error {
	if provider == nil {
		return errors.New("datafusion: table provider is nil")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return ErrSessionClosed
	}

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var supportsPushdown C.uint8_t
	if _, ok := provider.(PushdownTableProvider); ok {
		supportsPushdown = 1
	}

	// Ownership of the handle passes to the engine on entry: on success it
	// is released when the session is freed, on failure the engine calls
	// go_table_release before returning (see datafusion_go.h).
	handle := cgo.NewHandle(provider)
	cErr := C.df_session_register_table(s.handle, cName, C.uintptr_t(handle), supportsPushdown)
	return cErrorToGo(cErr)
}

// setCallbackError writes msg into the trampoline error out-param as a
// malloc-allocated C string (freed by the Rust side with free).
func setCallbackError(errOut **C.char, msg string) {
	if errOut != nil {
		*errOut = C.CString(msg)
	}
}

// trapCallbackPanic converts a panic in a Go callback into an error on
// the out-param, so nothing ever unwinds across the FFI boundary. Use as
// `defer trapCallbackPanic("...", errOut)`.
func trapCallbackPanic(what string, errOut **C.char) {
	if r := recover(); r != nil {
		setCallbackError(errOut, fmt.Sprintf("panic in Go %s: %v", what, r))
	}
}

// releaseCgoHandle deletes a cgo.Handle minted for a registered
// implementation (table, scalar UDF, catalog, or schema), used by every
// go_*_release trampoline. A bad handle must never panic across the FFI
// boundary during drop.
func releaseCgoHandle(handle C.uintptr_t) {
	defer func() { _ = recover() }()
	cgo.Handle(handle).Delete()
}

//export go_table_schema
func go_table_schema(handle C.uintptr_t, outSchema *C.struct_ArrowSchema, errOut **C.char) {
	defer trapCallbackPanic("TableProvider.Schema", errOut)

	provider := cgo.Handle(handle).Value().(TableProvider)
	schema := provider.Schema()
	if schema == nil {
		setCallbackError(errOut, "TableProvider.Schema returned nil")
		return
	}
	cdata.ExportArrowSchema(schema, (*cdata.CArrowSchema)(unsafe.Pointer(outSchema)))
}

//export go_table_scan
func go_table_scan(handle C.uintptr_t, projection *C.int32_t, projectionLen C.intptr_t,
	filtersJSON *C.char, limit C.int64_t,
	outStream *C.struct_ArrowArrayStream, errOut **C.char) {
	defer trapCallbackPanic("TableProvider.Scan", errOut)

	provider := cgo.Handle(handle).Value().(TableProvider)

	var reader array.RecordReader
	var err error
	if pd, ok := provider.(PushdownTableProvider); ok {
		opts := &ScanOptions{Limit: int64(limit)}
		if projectionLen >= 0 {
			opts.Projection = make([]int, int(projectionLen))
			if projectionLen > 0 {
				for i, idx := range unsafe.Slice((*int32)(projection), int(projectionLen)) {
					opts.Projection[i] = int(idx)
				}
			}
		}
		if filtersJSON != nil {
			// Serializer and parser ship in one binary, so a decode
			// failure is a bug: surface it loudly instead of scanning
			// with silently dropped filters (design D7).
			opts.Filters, err = decodeFilters([]byte(C.GoString(filtersJSON)))
			if err != nil {
				setCallbackError(errOut, err.Error())
				return
			}
		}
		reader, err = pd.ScanWithOptions(context.Background(), opts)
	} else {
		// Non-pushdown providers keep today's full scan; the engine
		// passes the no-pushdown sentinels and projects/filters itself.
		reader, err = provider.Scan(context.Background())
	}
	if err != nil {
		setCallbackError(errOut, err.Error())
		return
	}
	if reader == nil {
		setCallbackError(errOut, "TableProvider.Scan returned a nil reader without an error")
		return
	}
	// The exported stream takes ownership of the reader; its release
	// callback releases the reader when the engine is done with it.
	cdata.ExportRecordReader(reader, (*cdata.CArrowArrayStream)(unsafe.Pointer(outStream)))
}

// ProjectReader wraps a RecordReader so it yields exactly the columns at
// the given indices (into the wrapped reader's schema), in that order. It
// is the easy way for a PushdownTableProvider whose backing store cannot
// select columns natively to honor ScanOptions.Projection exactly.
// Ownership of the wrapped reader passes to the returned reader.
// ProjectReader panics if an index is out of range for the schema.
func ProjectReader(reader array.RecordReader, indices []int) array.RecordReader {
	schema := reader.Schema()
	fields := make([]arrow.Field, len(indices))
	for i, idx := range indices {
		if idx < 0 || idx >= schema.NumFields() {
			panic(fmt.Sprintf("datafusion: ProjectReader index %d out of range for schema with %d fields", idx, schema.NumFields()))
		}
		fields[i] = schema.Field(idx)
	}
	return &projectedReader{
		inner:   reader,
		schema:  arrow.NewSchema(fields, nil),
		indices: indices,
	}
}

type projectedReader struct {
	inner   array.RecordReader
	schema  *arrow.Schema
	indices []int
	cur     arrow.RecordBatch
}

func (p *projectedReader) Schema() *arrow.Schema { return p.schema }

func (p *projectedReader) Next() bool {
	if p.cur != nil {
		p.cur.Release()
		p.cur = nil
	}
	if !p.inner.Next() {
		return false
	}
	rec := p.inner.RecordBatch()
	cols := make([]arrow.Array, len(p.indices))
	for i, idx := range p.indices {
		cols[i] = rec.Column(idx)
	}
	// NewRecordBatch retains the columns, so the projected batch stays
	// valid independent of the inner reader's current record.
	p.cur = array.NewRecordBatch(p.schema, cols, rec.NumRows())
	return true
}

func (p *projectedReader) RecordBatch() arrow.RecordBatch { return p.cur }

// Record implements the deprecated accessor of array.RecordReader.
func (p *projectedReader) Record() arrow.RecordBatch { return p.cur }

func (p *projectedReader) Err() error { return p.inner.Err() }

func (p *projectedReader) Retain() { p.inner.Retain() }

func (p *projectedReader) Release() {
	if p.cur != nil {
		p.cur.Release()
		p.cur = nil
	}
	p.inner.Release()
}

//export go_table_release
func go_table_release(handle C.uintptr_t) {
	releaseCgoHandle(handle)
}
