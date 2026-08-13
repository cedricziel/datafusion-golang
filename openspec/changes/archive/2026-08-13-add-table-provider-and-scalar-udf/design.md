## Context

See proposal.md — Why. The walking skeleton (see `openspec/changes/archive/2026-08-13-add-walking-skeleton/design.md`) established: a thin hand-rolled C ABI over DataFusion (D1) rather than consuming `datafusion-ffi`'s `abi_stable` types directly; Arrow data crossing via the Arrow C Stream interface (D2); opaque boxed handles (D3); an error-string-out-param convention with `catch_unwind` (D4); one shared lazily-initialized multi-threaded tokio runtime, with Go's blocking `SQL()` call doing `runtime.block_on(...)` and fully collecting results before returning (D5, and its Non-Goals — no partial streaming out of `SQL()` yet); a Rust staticlib linked into the Go binary at final link time (D6); and a session guarded by a Go-side `RWMutex` where `SQL()` holds a read lock and `Close()` holds a write lock to drain in-flight calls (D7). This change reuses all seven decisions unchanged and adds the machinery for DataFusion to call back into Go.

## Goals / Non-Goals

**Goals:**

- Let a Go program register a table and have DataFusion pull rows from it during query execution.
- Let a Go program register a scalar function and have DataFusion invoke it during expression evaluation.
- Establish one callback-trampoline mechanism reused by both, since UDAFs/UDWFs/catalogs/planner hooks will need the same shape later.
- Keep every Go callback panic- and error-safe: nothing thrown from Go may unwind into Rust.

**Non-Goals:**

- Filter or projection pushdown into `Scan` (`supports_filters_pushdown` reports unsupported for every filter; DataFusion filters/projects after the scan).
- Aggregate or window UDFs, catalog providers, or any planner rewrite hook (later changes).
- Variadic-arity or generically-typed scalar functions — signature (argument types, return type) is fixed and declared at registration.
- Unregistering a table or function once registered (only whole-session `Close()` releases them, per D7).
- Optimizing callback dispatch (e.g. avoiding `spawn_blocking` overhead per batch) — correctness and safety first, revisit if profiling shows it matters.

## Decisions

**D1 — Callbacks are fixed `extern "C"` symbols exported by Go, not a function-pointer vtable passed at registration.**
Because the Rust crate is always a staticlib linked into the same final Go binary (D6 from the walking skeleton), the two sides share one link unit: the Rust object file can reference undefined symbols (`go_table_schema`, `go_table_scan`, `go_table_release`, `go_scalar_udf_invoke`, `go_scalar_udf_release`) that Go's cgo `//export` machinery resolves at final link time, the same way any cgo-exported function works. Every registered table/function of a given kind shares these same five symbols; what varies per registration is an opaque handle argument. This is simpler and has less surface area than a vtable of function pointers (which matters when the callee is `dlopen`ed at runtime — not our situation). Alternative considered: a struct of function pointers passed into `df_session_register_table`/`_scalar_udf` — rejected as unnecessary indirection given a single static link.

**D2 — Opaque handle is a Go `cgo.Handle` (via `runtime/cgo`), passed to Rust as a `uintptr`/`usize`.**
`cgo.Handle` is the standard, GC-safe way to hand a Go value (here: a `TableProvider` or `ScalarUDF` implementation) to C code as an opaque token — it prevents the Go GC from moving/collecting the referenced object while Rust holds the handle, without needing `unsafe.Pointer` into the Go heap. Each Rust-side wrapper (`GoTableProvider`, `GoScalarUdfImpl`) stores the `usize` and calls the matching `_release` extern function in its `Drop` impl, which does `cgo.Handle(h).Delete()` Go-side. Handle lifetime is therefore exactly Rust `Arc` lifetime, which DataFusion already manages via its catalog/registry — no separate bookkeeping needed.

**D3 — Schema and function signature are fetched once, at registration, and cached in the Rust wrapper — never during planning or scan.**
`RegisterTable` calls `go_table_schema` once and stores the resulting `SchemaRef` on `GoTableProvider`; its (synchronous, non-async) `TableProvider::schema()` trait method just returns the cached value. `RegisterScalarUDF` takes the declared argument/return types as part of the Go-side registration call (an `ArrowSchema` C struct whose fields describe each argument, per D5 below) and stores them; `ScalarUDFImpl::signature()`/`return_type()` are pure Rust. This satisfies the spec's "schema exposed before scan" / "type checking before invocation" requirements without any FFI call on the planning hot path, and keeps DataFusion's sync trait methods sync.

**D4 — `Scan` streams; `SQL()` still doesn't.**
Unlike `SessionContext.SQL()` (which fully collects results before returning per the walking skeleton's Non-Goals), a registered `TableProvider.Scan` returns a Go `array.RecordReader` that Rust imports as a genuine `FFI_ArrowArrayStream` and pulls from batch-by-batch as the query consumes it — reusing the exact export/import code path `SQL()` already established, just in the opposite direction (Go exports, Rust imports). This is the natural shape for a data source and costs nothing extra to build since the C Stream Interface plumbing already exists.

**D5 — Scalar UDF arguments cross as one bundled Arrow struct array; a fixed-arity FFI signature regardless of the SQL function's arity.**
`go_scalar_udf_invoke(handle, in_array *FFI_ArrowArray, in_schema *FFI_ArrowSchema, out_array *FFI_ArrowArray, out_schema *FFI_ArrowSchema, err_out **c_char)`: the input is a single Arrow struct array whose N children are the N argument columns for that batch (constructed Rust-side from the physical expression's evaluated `ColumnarValue`s), and the output is a single Arrow array of the declared return type. Go decodes the struct's children into the argument arrays its function signature expects, and encodes its one result array back. This keeps the extern "C" surface fixed regardless of how many arguments a given registered function takes, instead of needing variadic or per-arity generated signatures. Registration (`RegisterScalarUDF`) similarly describes the signature as a struct `ArrowSchema` whose fields are the argument types, plus one separate `ArrowSchema` for the return type.

**D6 — Every call from Rust into Go runs inside `tokio::task::spawn_blocking`.**
A Go implementation of `Scan` or a scalar function may do arbitrary work (disk I/O, network calls, heavy computation) and the FFI call itself is a blocking cgo call from whichever tokio worker thread happens to run it. Calling it inline would risk starving the shared runtime (D5 from the walking skeleton) that every concurrent session and query depends on. Wrapping each `go_table_schema` / `go_table_scan` (and each subsequent `get_next` pull on the resulting stream) / `go_scalar_udf_invoke` call in `spawn_blocking` moves it to tokio's blocking thread pool, isolating slow Go callbacks from the async worker threads. This is the direct mitigation for the biggest new risk this change introduces.

**D7 — Close-time safety is inherited from the walking skeleton's D7, not reimplemented.**
Because `SQL()` fully collects a query's results before returning (D5/Non-Goals, walking skeleton), by the time any `SQL()` call returns, every table scan and UDF invocation that query triggered has already completed. `Close()`'s existing write-lock (draining in-flight `SQL()` calls before proceeding) therefore already guarantees no registered Go callback can be invoked after `Close()` returns — dropping the `SessionContext` drops the catalog's `Arc<dyn TableProvider>`/`Arc<dyn ScalarUDFImpl>` references, which triggers the `_release` calls, only after every in-flight query (and thus every in-flight callback) has finished. No new synchronization is needed beyond what D7 already provides.

**D8 — Registering a duplicate name is a Go-side check, not left to DataFusion's own overwrite-on-register behavior.**
DataFusion's catalog APIs generally allow overwriting an existing table/function registration silently. The spec requires an error instead. `RegisterTable`/`RegisterScalarUDF` check for an existing name (via a DataFusion lookup call) before registering and return a Go error if found, rather than relying on engine behavior that may change across DataFusion versions.

## Risks / Trade-offs

- [`spawn_blocking` per callback adds latency (thread-pool handoff) to every scan batch pull and every UDF invocation] → acceptable for this phase; correctness and runtime-starvation-safety come first. Revisit only if profiling on a later change shows it dominates.
- [A Go `Scan` implementation that never terminates its `RecordReader` (or blocks forever) will leak a blocking-pool thread and hang the query indefinitely] → no cancellation wiring in this change (matches the walking skeleton's deferred `context.Context`/cancellation open question); document the expectation that `Scan` implementations must terminate, and revisit with real cancellation support alongside that open question.
- [Bundling scalar UDF arguments as a struct array (D5) adds a construct/destructure step per batch on both sides] → simpler than a variadic C signature or generated per-arity bindings; revisit only if it measurably matters.
- [A Go implementation shared across concurrent queries (D6/spec "Parallel queries invoking a registered extension") must itself be goroutine-safe] → this is a contract on the implementer, exactly like `http.Handler`; document it prominently on the `TableProvider`/`ScalarUDF` interfaces.
- [Extending the schema/type mapping (Arrow C Schema round-trip for standalone types, not just full streams) touches code the walking skeleton didn't need] → new but small; reuses `arrow-go`'s `cdata` package and arrow-rs's existing `FFI_ArrowSchema` support, both already Arrow-C-ABI compliant.

## Migration Plan

Additive to the walking skeleton; no existing behavior changes except the `sql-execution` delta (Close now also releases registered extensions — a strengthening, not a breaking change, since no extensions could be registered before this change). No rollback concerns beyond reverting the commits.

## Open Questions

- Whether `Scan`/UDF invocation should accept a `context.Context` for cancellation now or wait for the walking skeleton's deferred `SQLContext(ctx, query)` open question to be resolved together — deferring both to the same later change keeps cancellation semantics consistent across the whole API. Does not change this change's specs or task breakdown.
- Exact Rust-side `TableProvider`/`ScalarUDFImpl` trait method set to implement beyond the required minimum (e.g. `table_type()`, `Any`/downcasting boilerplate) is an implementation detail resolved during coding, not a design-level decision.
