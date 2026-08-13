# Tasks: add-table-provider-and-scalar-udf

## 1. Shared FFI groundwork

- [x] 1.1 Add standalone Arrow C Schema import/export helpers on both sides (Rust: `FFI_ArrowSchema` for a single type and for a struct-of-fields; Go: `cdata` schema export for a single `arrow.DataType` and for a struct built from named fields) — needed by both table registration (schema) and UDF registration (signature)
- [x] 1.2 Extend `include/datafusion_go.h` with the new C ABI: `df_session_register_table`, `df_session_register_scalar_udf`, and `extern` declarations for the five Go-exported trampoline symbols (`go_table_schema`, `go_table_scan`, `go_table_release`, `go_scalar_udf_invoke`, `go_scalar_udf_release`)
- [x] 1.3 Add a Rust helper to run a Go callback via `tokio::task::spawn_blocking` with `catch_unwind`-equivalent error propagation from the blocking call (design D6)

## 2. Table provider — Rust side

- [x] 2.1 Implement `GoTableProvider` wrapping a `cgo.Handle`-derived `usize`, caching the schema fetched once via `go_table_schema` at construction (design D3)
- [x] 2.2 Implement `TableProvider::scan` returning an `ExecutionPlan` whose stream pulls from the `FFI_ArrowArrayStream` produced by `go_table_scan`, each pull wrapped in `spawn_blocking` (design D4, D6)
- [x] 2.3 Report `Unsupported` from `supports_filters_pushdown` for every filter (no pushdown, per Non-Goals)
- [x] 2.4 Implement `Drop for GoTableProvider` calling `go_table_release`
- [x] 2.5 Implement `df_session_register_table`: reject duplicate names (design D8), construct `GoTableProvider`, register it on the session's catalog, `catch_unwind`-wrapped per the walking skeleton's D4
- [x] 2.6 Rust unit test: a fake Go-side table (stubbed trampoline functions) registered and queried via `SELECT * FROM` through the raw C ABI, including a duplicate-registration-is-rejected case and a scan-returns-error case

## 3. Table provider — Go side

- [x] 3.1 Write failing Go test: implement a `datafusion.TableProvider` backed by an in-memory Arrow table, `RegisterTable`, `SQL("SELECT * FROM t")`, assert results match
- [x] 3.2 Define `datafusion.TableProvider` interface (`Schema() *arrow.Schema`, `Scan(ctx context.Context) (array.RecordReader, error)`) and `SessionContext.RegisterTable(name string, provider TableProvider) error`
- [x] 3.3 Implement the `//export`ed trampolines (`goTableSchema`, `goTableScan`, `goTableRelease`) resolving the `cgo.Handle`, exporting the Go-side schema/stream via `cdata`, recovering panics into the error out-param (spec: errors and panics during scan)
- [x] 3.4 Make test 3.1 pass; add tests for: duplicate table name error, unknown-column query error without invoking scan, multi-batch scan, empty-table scan, scan-returns-error mid-stream leaves session usable, table not invoked after session `Close()`
- [x] 3.5 Add concurrency test: multiple goroutines querying the same registered table concurrently under `go test -race`

## 4. Scalar UDF — Rust side

- [x] 4.1 Implement `GoScalarUdfImpl` wrapping a `cgo.Handle`-derived `usize`, storing the argument/return `DataType`s supplied at registration (design D3)
- [x] 4.2 Implement `ScalarUDFImpl::invoke_with_args` (or the applicable current-DataFusion-version trait method): bundle the batch's argument `ColumnarValue`s into one Arrow struct array, call `go_scalar_udf_invoke` inside `spawn_blocking` (design D5, D6), unpack the returned array as the result
- [x] 4.3 Implement type checking: reject calls whose argument types don't match the declared signature during planning (via `signature()`/`return_type()`, not at invoke time)
- [x] 4.4 Implement `Drop for GoScalarUdfImpl` calling `go_scalar_udf_release`
- [x] 4.5 Implement `df_session_register_scalar_udf`: reject duplicate names (design D8), construct `GoScalarUdfImpl`, register it, `catch_unwind`-wrapped
- [x] 4.6 Rust unit test: a fake Go-side scalar function (stubbed trampoline) registered and called via `SELECT fn(...)` through the raw C ABI, including duplicate-registration-rejected and wrong-argument-type-rejected-at-planning cases

## 5. Scalar UDF — Go side

- [x] 5.1 Write failing Go test: implement a `datafusion.ScalarUDF` (e.g. `double(x int64) int64`), `RegisterScalarUDF`, `SQL("SELECT double(21)")`, assert result is 42
- [x] 5.2 Define `datafusion.ScalarUDF` interface/constructor (name, argument types, return type, `Evaluate(args []arrow.Array) (arrow.Array, error)`) and `SessionContext.RegisterScalarUDF(udf ScalarUDF) error`
- [x] 5.3 Implement the `//export`ed trampolines (`goScalarUdfInvoke`, `goScalarUdfRelease`) decoding the bundled struct array into argument arrays, encoding the result array, recovering panics into the error out-param (spec: errors and panics during evaluation)
- [x] 5.4 Make test 5.1 pass; add tests for: duplicate function name error, wrong-argument-type query rejected at planning, evaluation across multiple batches, function returns error, function panics, function not invoked after session `Close()`
- [x] 5.5 Add concurrency test: multiple goroutines calling the same registered function concurrently under `go test -race`

## 6. Example, docs, and wrap-up

- [x] 6.1 Extend `examples/sql` (or add `examples/extend`) demonstrating a registered table joined/filtered via SQL and a registered scalar UDF used in the same query
- [x] 6.2 Update README: extensibility section covering `RegisterTable`/`RegisterScalarUDF`, the goroutine-safety contract on implementations, and current limitations (no pushdown, no cancellation, fixed signatures)
- [x] 6.3 Run `make lint && make format`, ensure `go test ./... -race` and `cargo test` both pass, ensure clean tree
- [ ] 6.4 Commit series (semantic commits: FFI groundwork, Rust table provider, Go table provider, Rust scalar UDF, Go scalar UDF, example/docs)
