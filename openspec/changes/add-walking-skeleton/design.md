# Design: add-walking-skeleton

## Context

Greenfield repository. See proposal.md — Why. The long-term goal is full DataFusion extensibility from Go (UDFs, table providers, planner hooks), which forces bidirectional FFI; this change establishes the unidirectional half (Go → Rust → Arrow results) and the conventions everything later builds on. Constraints: cgo is unavoidable; Arrow data must cross the boundary zero-copy; DataFusion is async (tokio) while the Go API should be synchronous and blocking; nothing may panic or unwind across the FFI boundary.

## Goals / Non-Goals

**Goals:**

- Prove the full toolchain: Cargo staticlib → cgo link → Go test → runnable example.
- Establish the FFI conventions (opaque handles, error out-params, Arrow C Stream for results, explicit release) that later changes reuse unchanged.
- Keep the public Go API small and idiomatic: `datafusion.NewSessionContext()`, `ctx.SQL(query)`, `Close()` everywhere, `arrow-go` types for all data.

**Non-Goals:**

- Prebuilt binary distribution (separate change; developers need Rust + Go toolchains for now).
- Registering data sources, UDFs, or any Rust→Go callbacks.
- Streaming execution of partial results before the query completes (first version may buffer batches engine-side; the Go API shape — a RecordReader — already supports streaming, so tightening this later is invisible to callers).
- Windows support (macOS + Linux first; Windows linking has its own quirks).

## Decisions

**D1 — Custom thin C ABI over DataFusion, not `datafusion-ffi`'s abi_stable types directly.**
`datafusion-ffi` is designed for Rust↔Rust plugin boundaries using `abi_stable`; its types are not practical to consume from cgo. Instead the Rust crate exposes a handful of plain C functions (`df_session_new`, `df_session_sql`, `df_session_free`, `df_string_free`) and we reserve `datafusion-ffi` for later phases where Go-implemented providers/UDFs are handed back to DataFusion. Alternative considered: generating bindings to `datafusion-ffi` types — rejected for complexity and ABI fragility.

**D2 — Results cross as an Arrow C Stream (`FFI_ArrowArrayStream`).**
`df_session_sql` writes into a caller-allocated `ArrowArrayStream` struct; Go imports it with `arrow-go`'s `cdata.ImportCArrayStream`, yielding a standard `array.RecordReader`. Zero-copy, standard release-callback semantics, and the exact same shape later phases use for scans and exec nodes. Alternative: Arrow IPC bytes — simpler ownership but a full serialization pass; rejected.

**D3 — Opaque handles, never pointers to Rust objects' internals.**
Every Rust-side object crossing the boundary is `Box`ed and passed as an opaque `*mut c_void`. Go wraps handles in structs with a `Close() error` method and sets runtime finalizers as a leak backstop (finalizer logs; explicit Close is the contract, matching the spec's "explicit release" requirement).

**D4 — Error convention: return `*mut c_char` message, NULL on success.**
Each fallible C function returns a heap-allocated UTF-8 error string (freed by `df_string_free`) or NULL. Go converts to `error` via a small helper. All Rust entry points wrap their bodies in `catch_unwind`, so panics become error strings instead of UB across FFI. Alternative: error codes + thread-local last-error — more ceremony, no benefit at this call granularity.

**D5 — One shared multi-threaded tokio runtime per process, created lazily.**
A `static OnceLock<Runtime>` in the Rust crate; `df_session_sql` does `runtime.block_on(...)`. The calling goroutine blocks in cgo, which is fine — Go's scheduler spawns OS threads as needed, and this gives the synchronous API the spec promises while DataFusion still parallelizes internally. Session-per-runtime was rejected: heavier, and complicates the later story where callbacks must not deadlock the runtime.

**D6 — Repo layout and build wiring.**

```
datafusion/          Go package (public API + cgo glue in cgo.go)
rust/                Cargo workspace
  datafusion-c-abi/  the staticlib crate (crate-type = ["staticlib"])
examples/sql/        runnable example (main package)
Makefile             build, test, lint, format (per user convention)
include/datafusion_go.h   hand-written C header used by cgo
```

cgo directives point at the Cargo `target/release` output via `#cgo LDFLAGS`. The Makefile builds Rust first, then Go; `make test` runs both `cargo test` and `go test ./...`. The header is hand-written (5 functions) rather than cbindgen-generated — revisit when the surface grows.

**D7 — Concurrency: sessions are `Arc`-shared, `SessionContext` is internally thread-safe in DataFusion.**
The Go wrapper guards the handle with an atomic closed flag + `sync.RWMutex` so `Close` can drain in-flight calls, satisfying the use-after-close and concurrent-use spec requirements without a global lock around query execution.

## Risks / Trade-offs

- [cgo + static Rust lib link friction (macOS `-framework` needs, Linux `-lm -ldl`, symbol collisions)] → keep the ABI tiny, CI on both OSes from day one, document required toolchains in README.
- [`block_on` from many goroutines could exhaust tokio blocking budget or interact badly with later Rust→Go callbacks] → acceptable for this phase; D5 isolates the choice in one place so later phases can switch to a handoff (spawn + channel) without API change.
- [Buffering full results engine-side before returning the stream hides memory cost on large queries] → documented non-goal; the API shape already permits true streaming later.
- [Finalizers may mask leaked handles during development] → finalizer logs a warning in debug builds; tests assert explicit Close paths.
- [arrow-go and DataFusion's arrow-rs versions drift on C interface details] → both implement the frozen Arrow C ABI, which is stable by design; pin versions in go.mod/Cargo.lock regardless.

## Migration Plan

Greenfield — nothing to migrate. First commit lands the skeleton behind `make build && make test` green on macOS and Linux CI.

## Open Questions

- Whether to expose a `context.Context`-accepting `SQLContext(ctx, query)` variant with cancellation wired to DataFusion's cancellation in this change or the next (does not affect the current specs; additive).
- Exact minimum supported Rust/Go versions (pin once CI exists).
