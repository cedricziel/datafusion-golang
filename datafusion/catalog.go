package datafusion

/*
#include <stdlib.h>
#include <stdint.h>
#include "datafusion_go.h"
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/cgo"
	"unsafe"
)

// CatalogProvider is a Go-implemented catalog that DataFusion can query
// dynamically, making SQL able to address catalog.schema.table against
// content discovered at query time rather than declared up front via
// RegisterTable.
//
// Implementations must be safe for concurrent use by multiple goroutines,
// like TableProvider. Nothing is cached at registration time: SchemaNames
// and Schema are called fresh for every query that needs them, so an
// implementation that wants to avoid repeated work should memoize
// internally.
type CatalogProvider interface {
	// SchemaNames returns the catalog's current schema names.
	SchemaNames(ctx context.Context) ([]string, error)
	// Schema resolves name to a schema. found is false (not an error) when
	// no schema of that name exists; the engine turns that into its own
	// standard schema-not-found error.
	Schema(ctx context.Context, name string) (schema SchemaProvider, found bool, err error)
}

// SchemaProvider is a schema resolved from a CatalogProvider, exposing the
// tables it contains.
type SchemaProvider interface {
	// TableNames returns the schema's current table names.
	TableNames(ctx context.Context) ([]string, error)
	// Table resolves name to a table. found is false (not an error) when
	// no table of that name exists; the engine turns that into its own
	// standard table-not-found error. The returned TableProvider is
	// queried exactly like one passed to RegisterTable — the same
	// Schema/Scan (or ScanWithOptions, if it implements
	// PushdownTableProvider) contract applies.
	Table(ctx context.Context, name string) (table TableProvider, found bool, err error)
}

// RegisterCatalog registers a Go-implemented catalog under the given name,
// making catalog.schema.table queryable via SQL on this session.
//
// The engine's default catalog name is not special-cased: registering
// under it replaces the default catalog outright, and any tables or
// functions previously registered via RegisterTable/RegisterScalarUDF
// become unreachable through SQL as a result. Registering under any other
// name already in use returns an error and leaves the existing catalog
// unchanged.
func (s *SessionContext) RegisterCatalog(name string, catalog CatalogProvider) error {
	if catalog == nil {
		return errors.New("datafusion: catalog provider is nil")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return ErrSessionClosed
	}

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	// Ownership of the handle passes to the engine on entry: on success it
	// is released when the session is freed (or the catalog is replaced),
	// on failure the engine calls go_catalog_release before returning.
	handle := cgo.NewHandle(catalog)
	cErr := C.df_session_register_catalog(s.handle, cName, C.uintptr_t(handle))
	return cErrorToGo(cErr)
}

// encodeNameList JSON-encodes names as the array-of-strings document the
// Rust side decodes (the schema/table name-list convention).
func encodeNameList(names []string) (*C.char, error) {
	if names == nil {
		names = []string{}
	}
	data, err := json.Marshal(names)
	if err != nil {
		return nil, fmt.Errorf("encoding name list: %w", err)
	}
	return C.CString(string(data)), nil
}

//export go_catalog_schema_names
func go_catalog_schema_names(handle C.uintptr_t, outNamesJSON **C.char, errOut **C.char) {
	defer trapCallbackPanic("CatalogProvider.SchemaNames", errOut)

	provider := cgo.Handle(handle).Value().(CatalogProvider)
	names, err := provider.SchemaNames(context.Background())
	if err != nil {
		setCallbackError(errOut, err.Error())
		return
	}
	cNames, err := encodeNameList(names)
	if err != nil {
		setCallbackError(errOut, err.Error())
		return
	}
	*outNamesJSON = cNames
}

//export go_catalog_schema_lookup
func go_catalog_schema_lookup(handle C.uintptr_t, name *C.char,
	outSchemaHandle *C.uintptr_t, outFound *C.uint8_t, errOut **C.char) {
	defer trapCallbackPanic("CatalogProvider.Schema", errOut)

	provider := cgo.Handle(handle).Value().(CatalogProvider)
	schema, found, err := provider.Schema(context.Background(), C.GoString(name))
	if err != nil {
		setCallbackError(errOut, err.Error())
		return
	}
	if !found {
		*outFound = 0
		return
	}
	if schema == nil {
		setCallbackError(errOut, "CatalogProvider.Schema reported found with a nil SchemaProvider")
		return
	}
	*outFound = 1
	*outSchemaHandle = C.uintptr_t(cgo.NewHandle(schema))
}

//export go_catalog_release
func go_catalog_release(handle C.uintptr_t) {
	releaseCgoHandle(handle)
}

//export go_schema_table_names
func go_schema_table_names(handle C.uintptr_t, outNamesJSON **C.char, errOut **C.char) {
	defer trapCallbackPanic("SchemaProvider.TableNames", errOut)

	provider := cgo.Handle(handle).Value().(SchemaProvider)
	names, err := provider.TableNames(context.Background())
	if err != nil {
		setCallbackError(errOut, err.Error())
		return
	}
	cNames, err := encodeNameList(names)
	if err != nil {
		setCallbackError(errOut, err.Error())
		return
	}
	*outNamesJSON = cNames
}

//export go_schema_table_lookup
func go_schema_table_lookup(handle C.uintptr_t, name *C.char,
	outTableHandle *C.uintptr_t, outSupportsPushdown *C.uint8_t, outFound *C.uint8_t, errOut **C.char) {
	defer trapCallbackPanic("SchemaProvider.Table", errOut)

	provider := cgo.Handle(handle).Value().(SchemaProvider)
	table, found, err := provider.Table(context.Background(), C.GoString(name))
	if err != nil {
		setCallbackError(errOut, err.Error())
		return
	}
	if !found {
		*outFound = 0
		return
	}
	if table == nil {
		setCallbackError(errOut, "SchemaProvider.Table reported found with a nil TableProvider")
		return
	}
	*outFound = 1
	if _, ok := table.(PushdownTableProvider); ok {
		*outSupportsPushdown = 1
	} else {
		*outSupportsPushdown = 0
	}
	// Fed straight into the same GoTableProvider machinery a
	// RegisterTable-registered table uses; released via go_table_release,
	// not go_schema_release.
	*outTableHandle = C.uintptr_t(cgo.NewHandle(table))
}

//export go_schema_release
func go_schema_release(handle C.uintptr_t) {
	releaseCgoHandle(handle)
}
