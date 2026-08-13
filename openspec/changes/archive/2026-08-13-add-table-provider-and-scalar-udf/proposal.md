# Proposal: add-table-provider-and-scalar-udf

## Why

The walking skeleton (`sql-execution`) only proves Go → Rust → Arrow: Go calls into DataFusion and reads results. The whole point of this project is user-defined extensibility, which requires the reverse direction — DataFusion calling back into Go to fetch rows from a Go-implemented data source and to evaluate a Go-implemented function during query execution. This change adds the first two extension points (a table provider and a scalar UDF), establishing the callback-trampoline pattern (opaque Go-side handles via `cgo.Handle`, vtables of C function pointers, thread-safety for calls arriving on arbitrary tokio worker threads) that every later extension point (aggregates, window functions, catalogs, planner rules) will reuse.

## What Changes

- New Go interface `datafusion.TableProvider` with `Schema() *arrow.Schema` and `Scan(ctx) (array.RecordReader, error)`; a session can `RegisterTable(name string, provider TableProvider) error` making it queryable via SQL (`SELECT * FROM name`).
- New Go interface `datafusion.ScalarUDF` (or a function-adapter `datafusion.NewScalarUDF(name, ...)`) taking Arrow arrays in and returning an Arrow array out, batch-vectorized; a session can `RegisterScalarUDF(udf ScalarUDF) error` making it callable from SQL (`SELECT my_udf(col) FROM ...`).
- New Rust-side FFI machinery: a Go-backed `TableProvider` trait impl and a Go-backed `ScalarUDFImpl` trait impl, each dispatching through a vtable of C function pointers back to Go, carrying an opaque `cgo.Handle`-derived pointer as context.
- Table scan is full-table only in this change: no filter or projection pushdown (`TableProvider::supports_filters_pushdown` reports unsupported; DataFusion applies filters/projections after the scan). Pushdown is explicitly deferred.
- Scalar UDFs in this change are single-input-arity-agnostic but fixed-signature: declared input/output Arrow types, no variadic args, no `Volatility` control beyond a fixed default (`Volatile`, the safe default), no async evaluation.
- Panics inside Go callbacks and Go-side errors returned from a callback must not unwind into Rust or DataFusion; they surface as query execution errors.
- Extends the build to compile and test the new Rust callback machinery and the new Go interfaces together (a Go table provider queried by a Go-triggered SQL statement, round-tripping through Rust).

## Capabilities

### New Capabilities

- `table-provider`: Registering a Go-implemented table in a session and querying it via SQL, including schema exposure and full-table scan.
- `scalar-udf`: Registering a Go-implemented scalar function in a session and calling it from SQL, vectorized over Arrow batches.

### Modified Capabilities

- `sql-execution`: `SessionContext` gains `RegisterTable` and `RegisterScalarUDF`; a session must remain safe to close (releasing engine-side references to registered Go callbacks) and safe for concurrent use per the existing lifecycle/concurrency requirements, now extended to cover registered extensions.

## Impact

- **Code**: `datafusion/table.go` (TableProvider interface + registration), `datafusion/udf.go` (ScalarUDF interface + registration), `datafusion/cgo.go` extensions (vtable exports, handle plumbing), `rust/datafusion-c-abi/src/table.rs` and `.../src/udf.rs` (Go-backed trait impls), `include/datafusion_go.h` additions (registration functions, callback vtable structs).
- **Dependencies**: Rust — `datafusion-ffi` becomes relevant here (its `FFI_TableProvider`/`FFI_ScalarUDF` shapes inform, though our ABI stays our own thin C layer per the walking skeleton's D1 decision — revisit in design.md whether to adopt `datafusion-ffi` types now that a real callback boundary exists). Go — no new dependencies beyond `arrow-go` already in use.
- **Downstream**: Establishes the callback/threading/handle-lifetime conventions that UDAFs, window functions, catalog providers, and (eventually) planner hooks will all reuse; a design mistake here (e.g. handle lifetime, panic safety) propagates to every later extension point.
