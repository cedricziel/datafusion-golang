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

// RegisterTable registers a Go-implemented table under the given name,
// making it queryable via SQL on this session. Registering a name that is
// already in use returns an error and leaves the existing table
// unchanged. The engine holds a reference to the provider until the
// session is closed.
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

	// Ownership of the handle passes to the engine on entry: on success it
	// is released when the session is freed, on failure the engine calls
	// go_table_release before returning (see datafusion_go.h).
	handle := cgo.NewHandle(provider)
	cErr := C.df_session_register_table(s.handle, cName, C.uintptr_t(handle))
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
func go_table_scan(handle C.uintptr_t, outStream *C.struct_ArrowArrayStream, errOut **C.char) {
	defer trapCallbackPanic("TableProvider.Scan", errOut)

	provider := cgo.Handle(handle).Value().(TableProvider)
	reader, err := provider.Scan(context.Background())
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

//export go_table_release
func go_table_release(handle C.uintptr_t) {
	// Never let a bad handle panic across the FFI boundary during drop.
	defer func() { _ = recover() }()
	cgo.Handle(handle).Delete()
}
