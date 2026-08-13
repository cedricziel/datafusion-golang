package datafusion_test

import (
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

// newDoubleUDF returns a double(x BIGINT) -> BIGINT function and a
// counter of how many batches it evaluated.
func newDoubleUDF(t *testing.T, name string) (datafusion.ScalarUDF, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	udf, err := datafusion.NewScalarUDF(
		name,
		[]arrow.DataType{arrow.PrimitiveTypes.Int64},
		arrow.PrimitiveTypes.Int64,
		func(args []arrow.Array) (arrow.Array, error) {
			calls.Add(1)
			in := args[0].(*array.Int64)
			b := array.NewInt64Builder(memory.DefaultAllocator)
			defer b.Release()
			for i := 0; i < in.Len(); i++ {
				if in.IsNull(i) {
					b.AppendNull()
					continue
				}
				b.Append(in.Value(i) * 2)
			}
			return b.NewArray(), nil
		},
	)
	if err != nil {
		t.Fatalf("NewScalarUDF: %v", err)
	}
	return udf, &calls
}

func int64Results(t *testing.T, reader array.RecordReader) []int64 {
	t.Helper()
	defer reader.Release()
	var out []int64
	for reader.Next() {
		col := reader.RecordBatch().Column(0).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			out = append(out, col.Value(i))
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reading result: %v", err)
	}
	return out
}

func TestRegisterScalarUDF_RegisterAndCall(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	udf, calls := newDoubleUDF(t, "double")
	if err := ctx.RegisterScalarUDF(udf); err != nil {
		t.Fatalf("RegisterScalarUDF: %v", err)
	}

	reader, err := ctx.SQL("SELECT double(21) AS answer")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	got := int64Results(t, reader)
	if len(got) != 1 || got[0] != 42 {
		t.Fatalf("expected [42], got %v", got)
	}
	if calls.Load() == 0 {
		t.Fatalf("expected the Go function to be invoked")
	}
}

func TestRegisterScalarUDF_DuplicateNameFails(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	first, _ := newDoubleUDF(t, "double")
	if err := ctx.RegisterScalarUDF(first); err != nil {
		t.Fatalf("RegisterScalarUDF: %v", err)
	}
	second, _ := newDoubleUDF(t, "double")
	err = ctx.RegisterScalarUDF(second)
	if err == nil {
		t.Fatalf("expected duplicate registration to fail")
	}
	if !strings.Contains(err.Error(), "already") {
		t.Fatalf("expected duplicate-name error, got: %v", err)
	}

	// original function must remain callable
	reader, err := ctx.SQL("SELECT double(5)")
	if err != nil {
		t.Fatalf("SQL after failed duplicate registration: %v", err)
	}
	if got := int64Results(t, reader); len(got) != 1 || got[0] != 10 {
		t.Fatalf("expected [10], got %v", got)
	}
}

func TestRegisterScalarUDF_WrongArgumentTypeRejectedAtPlanning(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	udf, calls := newDoubleUDF(t, "double")
	if err := ctx.RegisterScalarUDF(udf); err != nil {
		t.Fatalf("RegisterScalarUDF: %v", err)
	}

	_, err = ctx.SQL("SELECT double('not a number')")
	if err == nil {
		t.Fatalf("expected planning error for wrong argument type")
	}
	if calls.Load() != 0 {
		t.Fatalf("function must not be invoked when planning rejects the call, got %d", calls.Load())
	}
}

func TestRegisterScalarUDF_EvaluatedAcrossMultipleBatches(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	// the multi-batch input comes from a registered Go table
	schema := peopleSchema()
	table := newPeopleTable(t,
		peopleBatch(t, schema, []int64{1, 2}, []string{"a", "b"}),
		peopleBatch(t, schema, []int64{3, 4}, []string{"c", "d"}),
	)
	if err := ctx.RegisterTable("people", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}
	udf, calls := newDoubleUDF(t, "double")
	if err := ctx.RegisterScalarUDF(udf); err != nil {
		t.Fatalf("RegisterScalarUDF: %v", err)
	}

	reader, err := ctx.SQL("SELECT double(id) FROM people ORDER BY 1")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	got := int64Results(t, reader)
	want := []int64{2, 4, 6, 8}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
	if calls.Load() < 2 {
		t.Fatalf("expected one invocation per batch (>= 2), got %d", calls.Load())
	}
}

func TestRegisterScalarUDF_EvaluationErrorLeavesSessionUsable(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	udf, err := datafusion.NewScalarUDF(
		"failing",
		[]arrow.DataType{arrow.PrimitiveTypes.Int64},
		arrow.PrimitiveTypes.Int64,
		func(args []arrow.Array) (arrow.Array, error) {
			return nil, errors.New("evaluation refused by Go")
		},
	)
	if err != nil {
		t.Fatalf("NewScalarUDF: %v", err)
	}
	if err := ctx.RegisterScalarUDF(udf); err != nil {
		t.Fatalf("RegisterScalarUDF: %v", err)
	}

	_, err = ctx.SQL("SELECT failing(1)")
	if err == nil || !strings.Contains(err.Error(), "evaluation refused by Go") {
		t.Fatalf("expected the Go error to surface, got: %v", err)
	}

	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("SQL after evaluation error: %v", err)
	}
	reader.Release()
}

func TestRegisterScalarUDF_PanicSurfacesAsError(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	udf, err := datafusion.NewScalarUDF(
		"panicking",
		[]arrow.DataType{arrow.PrimitiveTypes.Int64},
		arrow.PrimitiveTypes.Int64,
		func(args []arrow.Array) (arrow.Array, error) {
			panic("udf panicked in Go")
		},
	)
	if err != nil {
		t.Fatalf("NewScalarUDF: %v", err)
	}
	if err := ctx.RegisterScalarUDF(udf); err != nil {
		t.Fatalf("RegisterScalarUDF: %v", err)
	}

	_, err = ctx.SQL("SELECT panicking(1)")
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("expected the panic to surface as an error, got: %v", err)
	}

	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("SQL after panic: %v", err)
	}
	reader.Release()
}

func TestRegisterScalarUDF_NotInvokedAfterClose(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}

	udf, calls := newDoubleUDF(t, "double")
	if err := ctx.RegisterScalarUDF(udf); err != nil {
		t.Fatalf("RegisterScalarUDF: %v", err)
	}
	if err := ctx.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := ctx.SQL("SELECT double(1)"); !errors.Is(err, datafusion.ErrSessionClosed) {
		t.Fatalf("expected ErrSessionClosed, got: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("function must not run after Close, got %d calls", calls.Load())
	}

	other, _ := newDoubleUDF(t, "other")
	if err := ctx.RegisterScalarUDF(other); !errors.Is(err, datafusion.ErrSessionClosed) {
		t.Fatalf("expected ErrSessionClosed for registration after Close, got: %v", err)
	}
}

func TestRegisterScalarUDF_ConcurrentCalls(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	udf, _ := newDoubleUDF(t, "double")
	if err := ctx.RegisterScalarUDF(udf); err != nil {
		t.Fatalf("RegisterScalarUDF: %v", err)
	}

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reader, err := ctx.SQL(fmt.Sprintf("SELECT double(%d)", i))
			if err != nil {
				errs <- err
				return
			}
			defer reader.Release()
			var got []int64
			for reader.Next() {
				col := reader.RecordBatch().Column(0).(*array.Int64)
				for j := 0; j < col.Len(); j++ {
					got = append(got, col.Value(j))
				}
			}
			if err := reader.Err(); err != nil {
				errs <- err
				return
			}
			if len(got) != 1 || got[0] != int64(i*2) {
				errs <- fmt.Errorf("goroutine %d: expected [%d], got %v", i, i*2, got)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("goroutine error: %v", err)
	}
}
