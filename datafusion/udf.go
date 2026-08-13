package datafusion

/*
#include <stdlib.h>
#include <stdint.h>
#include "datafusion_go.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"runtime/cgo"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

// ScalarUDF is a Go-implemented scalar function that can be registered on
// a SessionContext and called from SQL. Its signature is fixed at
// construction: the declared argument types and return type are enforced
// by the engine during query planning, before Evaluate is ever called.
//
// Implementations must be safe for concurrent use by multiple goroutines:
// the engine may evaluate batches from multiple engine-internal threads
// at once when concurrent queries call the same function.
type ScalarUDF interface {
	// Name is the SQL-callable function name. Unquoted SQL identifiers
	// are looked up lowercase, so use a lowercase name unless callers
	// will quote it.
	Name() string
	// ArgTypes declares the argument types, in order.
	ArgTypes() []arrow.DataType
	// ReturnType declares the result type.
	ReturnType() arrow.DataType
	// Evaluate computes one output value per input row, vectorized over
	// a batch: args holds one array per declared argument, all of equal
	// length, and the returned array must have that same length and the
	// declared return type. The argument arrays are only valid during
	// the call; the returned array's ownership passes to the engine (so
	// return a freshly built array, or Retain an input before returning
	// it).
	Evaluate(args []arrow.Array) (arrow.Array, error)
}

type scalarUDF struct {
	name       string
	argTypes   []arrow.DataType
	returnType arrow.DataType
	fn         func(args []arrow.Array) (arrow.Array, error)
}

func (u *scalarUDF) Name() string               { return u.name }
func (u *scalarUDF) ArgTypes() []arrow.DataType { return u.argTypes }
func (u *scalarUDF) ReturnType() arrow.DataType { return u.returnType }
func (u *scalarUDF) Evaluate(args []arrow.Array) (arrow.Array, error) {
	return u.fn(args)
}

// NewScalarUDF builds a ScalarUDF from a name, a fixed signature, and a
// batch-vectorized evaluation function. See the ScalarUDF interface for
// the contracts fn must uphold.
func NewScalarUDF(name string, argTypes []arrow.DataType, returnType arrow.DataType, fn func(args []arrow.Array) (arrow.Array, error)) (ScalarUDF, error) {
	if name == "" {
		return nil, errors.New("datafusion: scalar UDF name is empty")
	}
	if returnType == nil {
		return nil, errors.New("datafusion: scalar UDF return type is nil")
	}
	for i, dt := range argTypes {
		if dt == nil {
			return nil, fmt.Errorf("datafusion: scalar UDF argument type %d is nil", i)
		}
	}
	if fn == nil {
		return nil, errors.New("datafusion: scalar UDF evaluation function is nil")
	}
	return &scalarUDF{name: name, argTypes: argTypes, returnType: returnType, fn: fn}, nil
}

// RegisterScalarUDF registers a Go-implemented scalar function on this
// session, making it callable from SQL by its name. Registering a name
// that is already in use (including built-in function names) returns an
// error and leaves the existing function unchanged. The engine holds a
// reference to the function until the session is closed.
func (s *SessionContext) RegisterScalarUDF(udf ScalarUDF) error {
	if udf == nil {
		return errors.New("datafusion: scalar UDF is nil")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return ErrSessionClosed
	}

	cName := C.CString(udf.Name())
	defer C.free(unsafe.Pointer(cName))

	// The signature crosses as two struct-typed Arrow C Schemas: one whose
	// fields are the argument types, one with a single field for the
	// return type (design D5). Both are consumed by the engine during the
	// call regardless of outcome.
	argFields := make([]arrow.Field, len(udf.ArgTypes()))
	for i, dt := range udf.ArgTypes() {
		argFields[i] = arrow.Field{Name: fmt.Sprintf("arg%d", i), Type: dt, Nullable: true}
	}
	retFields := []arrow.Field{{Name: "return", Type: udf.ReturnType(), Nullable: true}}

	var cArgs, cRet C.struct_ArrowSchema
	cdata.ExportArrowSchema(arrow.NewSchema(argFields, nil), (*cdata.CArrowSchema)(unsafe.Pointer(&cArgs)))
	cdata.ExportArrowSchema(arrow.NewSchema(retFields, nil), (*cdata.CArrowSchema)(unsafe.Pointer(&cRet)))

	// Ownership of the handle passes to the engine on entry: on success it
	// is released when the session is freed, on failure the engine calls
	// go_scalar_udf_release before returning (see datafusion_go.h).
	handle := cgo.NewHandle(udf)
	cErr := C.df_session_register_scalar_udf(s.handle, cName, &cArgs, &cRet, C.uintptr_t(handle))
	return cErrorToGo(cErr)
}

//export go_scalar_udf_invoke
func go_scalar_udf_invoke(handle C.uintptr_t, argsArr *C.struct_ArrowArray, argsSchema *C.struct_ArrowSchema, outArr *C.struct_ArrowArray, outSchema *C.struct_ArrowSchema, errOut **C.char) {
	defer trapCallbackPanic("ScalarUDF.Evaluate", errOut)

	udf := cgo.Handle(handle).Value().(ScalarUDF)

	// Import takes ownership of the bundled argument struct array (and
	// releases the schema), matching the engine-side contract.
	_, bundled, err := cdata.ImportCArray(
		(*cdata.CArrowArray)(unsafe.Pointer(argsArr)),
		(*cdata.CArrowSchema)(unsafe.Pointer(argsSchema)),
	)
	if err != nil {
		setCallbackError(errOut, fmt.Sprintf("importing UDF arguments: %v", err))
		return
	}
	defer bundled.Release()

	structArr, ok := bundled.(*array.Struct)
	if !ok {
		setCallbackError(errOut, fmt.Sprintf("expected a struct array of arguments, got %T", bundled))
		return
	}
	args := make([]arrow.Array, structArr.NumField())
	for i := range args {
		args[i] = structArr.Field(i)
	}

	result, err := udf.Evaluate(args)
	if err != nil {
		setCallbackError(errOut, err.Error())
		return
	}
	if result == nil {
		setCallbackError(errOut, "ScalarUDF.Evaluate returned a nil array without an error")
		return
	}
	defer result.Release()
	cdata.ExportArrowArray(result,
		(*cdata.CArrowArray)(unsafe.Pointer(outArr)),
		(*cdata.CArrowSchema)(unsafe.Pointer(outSchema)),
	)
}

//export go_scalar_udf_release
func go_scalar_udf_release(handle C.uintptr_t) {
	releaseCgoHandle(handle)
}
