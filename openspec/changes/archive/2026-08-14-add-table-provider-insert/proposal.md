# Proposal: add-table-provider-insert

## Why

Every capability built so far is read-only: a Go program can register a `TableProvider` (with optional scan pushdown) and query it via SQL, but `INSERT INTO` against a registered table fails with DataFusion's default "Insert into not implemented for this table" error. Real backing stores now exist (`providers/parquet`, `providers/iceberg`) whose natural next step is accepting writes, and the engine has no channel to deliver DataFusion's insert input to Go code at all. This change adds that engine-level channel, following the same opt-in shape scan pushdown established.

## What Changes

- A Go table provider can opt into accepting `INSERT` statements by implementing a new optional interface (working name `WritableTableProvider`) with an `InsertInto(ctx, op, reader)` method alongside the existing `TableProvider`. Providers that implement only `TableProvider` (or `PushdownTableProvider`) keep today's behavior: `INSERT INTO` against them fails with a clear "not writable" query error, and reads are untouched.
- The opt-in is detected at registration time by type assertion, exactly like `PushdownTableProvider` — no separate registration call or configuration.
- On `INSERT INTO t ...` (any input DataFusion can plan: `VALUES` lists, `SELECT` from other tables, including other registered Go tables), the engine executes the statement's input plan and delivers the resulting rows to the provider as an Arrow record batch stream whose schema is the table's registered schema. DataFusion plans the input side itself — column reordering from an explicit column list, NULL/default filling for omitted columns, and casts to the registered column types all happen in the engine before rows reach the provider.
- The insert mode crosses as a parameter: `Append` (`INSERT INTO`), `Overwrite` (`INSERT OVERWRITE`), and `Replace` (`REPLACE INTO`), mirroring DataFusion 54.1.0's `InsertOp` enum. The engine plumbs whichever mode the statement produced; the provider decides which modes it supports and returns an error for the rest.
- The provider reports how many rows it wrote; the statement's SQL result is DataFusion's standard DML result — a single row with a `count` (UInt64) column — surfaced through the existing `SessionContext.SQL()` return value unchanged.
- One new FFI trampoline symbol (`go_table_insert`) carries the insert: data flows Rust→Go here (the engine produces the stream, Go consumes it), the reverse of `go_table_scan`, so it cannot ride the existing scan symbol. All established FFI conventions apply: fixed extern "C" symbol resolved at static link time, `cgo.Handle` opaque token, the call isolated on the blocking pool, panic-safety via the error out-param, and session-close draining.
- An error from the provider's insert (including a panic, converted at the boundary) fails the statement with a Go error and leaves the session usable. Whether rows written before the error remain visible is the provider's contract to define — the engine delivers the stream exactly once per statement and the provider must commit or roll back before returning, mirroring the position the existing spec takes on mid-scan errors.
- `providers/parquet` and `providers/iceberg` are NOT made writable in this change (explicit Non-Goal; follow-up work, exactly as `add-table-provider-pushdown` shipped the engine capability and `wire-parquet-and-iceberg-pushdown` wired the real providers separately). An in-repo test provider proves the capability end to end.

## Capabilities

### New Capabilities

None. Like scan pushdown before it (`add-table-provider-pushdown`), insert support is a contract between the engine and a registered table provider — another optional facet of the same capability, registered through the same call, released by the same session lifecycle. Splitting it into a standalone capability would duplicate the registration, error-handling, and lifecycle requirements that `table-provider` already owns.

### Modified Capabilities

- `table-provider`: new requirements covering the opt-in writable provider (declaration by type assertion at registration), delivery of the insert input stream in the registered schema, insert-mode plumbing, the row-count result contract, insert error handling, and session lifecycle for writes. The existing read-path requirements are unchanged.

## Impact

- **Code**: `datafusion/table.go` (new `WritableTableProvider` interface, `InsertOp` Go type, registration-time capability detection, new `go_table_insert` trampoline); `rust/datafusion-c-abi/src/table.rs` (implement `TableProvider::insert_into` on `GoTableProvider` via a `DataSink`/`DataSinkExec` pair that bridges the input plan's async stream to the synchronous Arrow C Stream the Go side consumes); `rust/datafusion-c-abi/src/ffi.rs` (new extern declaration + test stub); `include/datafusion_go.h` (extended `df_session_register_table` signature, new `go_table_insert` declaration); tests in `datafusion/table_test.go` and Rust `table.rs`.
- **Dependencies**: none new on either side. The Arrow C Stream machinery, `spawn_blocking` isolation, and error-string convention already exist; `DataSinkExec` ships with the pinned `datafusion = "54.1.0"`.
- **Downstream**: unlocks making `providers/parquet` and `providers/iceberg` writable as follow-up changes; establishes the Rust→Go data-flow direction any later write-shaped surface (DELETE/UPDATE, COPY INTO) would reuse.
