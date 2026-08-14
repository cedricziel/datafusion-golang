# Design: add-table-provider-insert

## Context

See proposal.md — Why. The established machinery this change extends (see `openspec/changes/archive/2026-08-13-add-table-provider-and-scalar-udf/design.md`, decisions D1–D8, and `openspec/changes/archive/2026-08-13-add-table-provider-pushdown/design.md`): fixed `extern "C"` trampoline symbols resolved at static-link time (prior D1), `cgo.Handle` opaque tokens (prior D2), schema fetched once at registration (prior D3), Arrow C Streams for bulk data (prior D4), every Rust→Go call on `spawn_blocking` (prior D6), close-time safety from the session's RWMutex draining plus fully-collecting `SQL()` (prior D7), Go-side duplicate-name check (prior D8), and registration-time opt-in by type assertion with in-place FFI signature extension (pushdown D4/D5). References like "prior D6" below point at those documents.

Verified facts about the pinned `datafusion = "54.1.0"` (from the vendored sources, not memory):

- `TableProvider::insert_into(&self, state: &dyn Session, input: Arc<dyn ExecutionPlan>, insert_op: InsertOp) -> Result<Arc<dyn ExecutionPlan>>` is an async trait method whose default implementation returns `not_impl_err!("Insert into not implemented for this table")` (`datafusion-catalog-54.1.0/src/table.rs:340-346`). There is no separate capability-declaration method: the physical planner (`datafusion-54.1.0/src/physical_planner.rs:836-851`) downcasts the DML target to `DefaultTableSource` and calls `insert_into` directly, so not overriding it is what makes a table read-only today.
- `InsertOp` (`datafusion-expr-54.1.0/src/logical_plan/dml.rs:264-275`) has exactly three variants: `Append` (SQL `INSERT INTO`), `Overwrite` (SQL `INSERT OVERWRITE`), `Replace` (SQL `REPLACE INTO`). `INSERT OVERWRITE` parses in every sqlparser dialect (the keyword is consumed unconditionally in `parse_insert`, sqlparser-0.62.0), so at least `Append` and `Overwrite` are reachable from plain SQL on our default session.
- `input` is the statement's fully planned source: the SQL planner (`datafusion-sql-54.1.0/src/statement.rs:2358-2512`, `insert_to_plan`) verifies the target exists, resolves an explicit column list against the table schema (rejecting unknown and duplicate columns), and wraps the source query in a projection that reorders columns to the registered order, fills omitted columns with the column default or a NULL literal, and casts every value to the registered column type. Both `VALUES` and `SELECT` sources flow through this one path. The input plan's schema therefore already matches the registered table schema when `insert_into` runs.
- The returned `ExecutionPlan` must produce a single row with a non-nullable UInt64 column named `count` (documented on the trait; `make_count_schema`/`make_count_batch` in `datafusion-datasource-54.1.0/src/sink.rs:274-289`).
- The reference implementation pattern is `DataSinkExec` + a `DataSink` impl: `MemTable::insert_into` (`datafusion-catalog-54.1.0/src/memory/table.rs:278-296`) checks `logically_equivalent_names_and_types` against the input schema, rejects non-`Append` ops with `not_impl_err`, and returns `DataSinkExec::new(input, Arc::new(MemSink), None)`. `DataSinkExec::execute` (`sink.rs:228-257`) drives `execute_input_stream(input, sink_schema, 0, ctx)` — which also enforces not-null constraints per batch when the sink schema is stricter than the input's — and calls `DataSink::write_all(data: SendableRecordBatchStream, ctx) -> Result<u64>` **exactly once per DML statement**; the trait documents that the sink must do any commit or rollback before returning. `write_all`'s returned count becomes the `count` batch.
- `DataSinkExec` requires a single input partition; the physical optimizer inserts the merge when needed (documented on `DataSinkExec::new`), so the sink sees one ordered stream.
- Our `df_session_sql` runs `runtime().block_on(async { ... df.collect().await ... })` (`rust/datafusion-c-abi/src/lib.rs:100-124`): executing the statement — including the entire `write_all` — completes before `df_session_sql` returns the (already collected) result stream. An `INSERT` therefore surfaces to Go's `SQL()` as a one-row, one-column `count` result through the unchanged result path.

## Goals / Non-Goals

**Goals:**

- Let a Go program accept `INSERT INTO` / `INSERT OVERWRITE` / `REPLACE INTO` rows into a registered table, with the same opt-in ergonomics as scan pushdown and zero behavior change for providers that don't opt in.
- Deliver the insert input as a real Arrow C Stream the Go provider consumes batch by batch — no full materialization on the Rust side — establishing the Rust-produces/Go-consumes stream direction for future write-shaped surfaces.
- Preserve every existing safety property: no Go callback outlives its statement, no panic crosses the boundary, no tokio worker blocks on Go code, sessions stay usable after any insert failure.

**Non-Goals:**

- Making `providers/parquet` or `providers/iceberg` writable (follow-up changes, exactly like `wire-parquet-and-iceberg-pushdown` followed `add-table-provider-pushdown`).
- `DELETE` / `UPDATE` support (`TableProvider::delete_from` / `update` exist in 54.1.0 and would follow this change's pattern later), `CREATE TABLE AS SELECT` into Go providers, and `COPY ... TO`.
- Transactional guarantees across statements or providers. The atomicity of one insert is the provider's contract (mirroring `DataSink`'s own commit-before-return wording); the engine guarantees only exactly-once delivery of the input stream.
- Constraint enforcement beyond what DataFusion already does on the input plan (type casts, not-null checks from `execute_input_stream`). No uniqueness or key semantics — `Replace` is delivered as a mode, its meaning is the provider's.
- Cancellation of an in-flight insert (rides the project-wide deferred `context.Context` open question, same as scans).
- Multi-partition / parallel sink writes and insert-side statistics or metrics reporting.

## Decisions

**D1 — Rust implements `insert_into` as `DataSinkExec` over a `GoDataSink`; no hand-rolled exec node.**
`GoTableProvider::insert_into` (when the provider opted in, see D3) validates the input schema against the cached registered schema (`logically_equivalent_names_and_types`, same defense as `MemTable` even though the SQL planner already projects/casts) and returns `DataSinkExec::new(input, Arc::new(GoDataSink { handle, schema, insert_op }), None)`. This is the pattern every built-in writable table uses and it buys the correct behavior for free: the `count` result schema and batch, single-partition input handling, per-batch not-null enforcement via `execute_input_stream`, and the "write_all called exactly once, commit/rollback before return" contract that maps one-to-one onto the Go interface's semantics. Alternative — a bespoke `ExecutionPlan` like `GoTableExec`: rejected; it would re-implement all of the above to end up isomorphic to `DataSinkExec`, and the scan side only hand-rolled its exec node because there is no equivalent read-side helper.

**D2 — One new trampoline symbol, `go_table_insert`, called once per statement with the whole input as an Arrow C Stream.**

```c
extern void go_table_insert(uintptr_t handle,
                            struct ArrowArrayStream *in_stream, /* ownership transfers to Go */
                            int32_t insert_op,                  /* 0 Append, 1 Overwrite, 2 Replace */
                            uint64_t *rows_out,
                            char **error_out);
```

A new symbol, not an extension of `go_table_scan` (the pushdown precedent extended in place): scan and insert are different operations with opposite data direction — in a scan Go produces the stream and Rust consumes it; here Rust produces and Go consumes. Overloading one symbol would mean a mode flag and two disjoint parameter sets, which is exactly the "dead parameter set on every call" the pushdown design rejected. Per-statement (not per-batch) granularity mirrors `DataSink::write_all` being called exactly once and gives the Go provider a natural transaction boundary: open, consume the reader to completion, commit or roll back, return the count. Ownership of `in_stream` transfers to Go on entry (the Go trampoline imports it as an `array.RecordReader` via the same `cdata` path `SQL()` results already use, and releases it when done — the reverse of `go_table_scan`'s export). `rows_out` is written only on success; `error_out` follows the existing malloc'd-C-string convention. Alternative — per-batch callbacks (`go_table_insert_begin` / `_write_batch` / `_commit`): rejected; three symbols, Rust-side state machine, and it re-invents exactly what the C Stream Interface already is.

**D3 — Opt-in is a registration-time boolean via type assertion; `df_session_register_table` extended in place.**
Go side mirrors `PushdownTableProvider` exactly (pushdown D4):

```go
type InsertOp int32

const (
    InsertAppend    InsertOp = 0 // INSERT INTO
    InsertOverwrite InsertOp = 1 // INSERT OVERWRITE
    InsertReplace   InsertOp = 2 // REPLACE INTO
)

type WritableTableProvider interface {
    TableProvider
    // InsertInto consumes rows (ownership of the reader passes to the
    // implementation, which must release it) and returns the number of
    // rows written. Called at most once per INSERT statement; commit or
    // roll back before returning.
    InsertInto(ctx context.Context, op InsertOp, rows array.RecordReader) (uint64, error)
}
```

`RegisterTable` type-asserts and passes a `supports_insert` byte through `df_session_register_table(session, name, handle, supports_pushdown, supports_insert)` — extended in place, both sides being one link unit (pushdown D5's argument verbatim). `GoTableProvider` stores the flag; `insert_into` returns a "table `<name>` does not support INSERT" `not_impl` error without any FFI call when it is unset, keeping non-writable providers on today's error path with a clearer message. The two capabilities compose freely: a provider may implement either optional interface, both, or neither. Alternative — an explicit registration flag/options struct: rejected; the type-assertion pattern is established, and pushdown D4's reasoning (capability determined solely from the provider value) already became a spec requirement.

**D4 — The async input stream reaches Go through a bounded-channel bridge; `write_all` returns only after both the Go call and the bridge have fully terminated.**
`DataSink::write_all` receives an async `SendableRecordBatchStream`, but the C Stream Interface Go consumes is synchronous pull. Bridge: `write_all` spawns a pump task on the shared runtime that forwards each batch (or the input's error) into a small bounded channel; a synchronous `RecordBatchReader` wrapping the receiving end (blocking receive — safe on the blocking pool, where the Go call runs) is exported as the `FFI_ArrowArrayStream` passed to `go_table_insert`, which itself runs under `spawn_go_blocking` (prior D6). An input-side error (e.g. a failing `SELECT` source, including another Go table's scan error) surfaces through the stream's `get_next` error path, so the provider sees a read error mid-consume and aborts; `write_all` then reports the input error. Critically, `write_all` awaits the pump task's completion before returning even when the Go call fails early: dropping the receiver makes the pump's next send fail and exit, but its in-flight pull from the input plan (possibly a `spawn_blocking` scan of another Go table) must finish before the statement is allowed to complete — see D6. Alternatives: *collect-then-send* (materialize all input batches, export a `RecordBatchIterator`) — rejected as the designed shape because `INSERT INTO t SELECT ... FROM big_table` would buffer the entire source in memory, though it is an acceptable fallback if the bridge proves finicky, since the observable contract (one stream, registered schema) is identical; *`Handle::block_on` inside the stream's `get_next` on the blocking-pool thread* — works today but couples correctness to "this thread is never a runtime worker", a global property a future refactor could silently break, whereas the channel bridge is locally verifiable.

**D5 — All three `InsertOp` variants pass through as an `int32`; the engine never filters modes.**
Rust maps `InsertOp::{Append, Overwrite, Replace}` to `0/1/2`; the Go trampoline maps them onto the `InsertOp` constants. The engine has no opinion about which modes a table supports — the provider returns an error for modes it doesn't implement, which fails the statement like any other insert error. This matches how `MemTable` handles it (rejects non-`Append` itself, not via planner machinery) and keeps the FFI surface stable when providers grow overwrite support. Both sides are one link unit, so an unknown value is unreachable; the trampoline still surfaces one as an error rather than guessing, per the existing "decode failure is a bug to surface loudly" convention.

**D6 — Prior D7's close-safety argument still holds for writes, with one new obligation discharged inside `write_all`.**
Re-derived rather than assumed: `df_session_sql` fully executes the statement inside `block_on` before returning (verified, Context), and for an `INSERT` that execution awaits `DataSinkExec::execute`'s stream, which awaits `write_all`, which awaits both the `spawn_go_blocking(go_table_insert ...)` call *and* the pump task (D4). So when `SQL()` returns, no insert callback and no input-side scan callback can still be running, and `Close()`'s existing write-lock drain retains its guarantee that no Go callback survives it. The one place the old argument was insufficient is the pump task: a detached task pulling from the input plan could outlive `write_all`'s early-error return and still be mid-`go_table_scan` when the session is freed. D4's "await the pump before returning" closes exactly that hole; it is a hard requirement on the implementation, not an optimization. `RegisterTable`'s existing closed-session check covers registration; no new synchronization is added anywhere.

**D7 — The reported row count is the provider's word, forwarded verbatim.**
`GoDataSink::write_all` returns `*rows_out` as its `u64`, which `DataSinkExec` wraps into the standard `count` batch. The engine does not count rows itself or cross-check the provider (it cannot know what "written" means for a deduplicating or upserting store — e.g. `Replace` may legitimately write fewer rows than it consumed). A wrong count is cosmetic: it cannot corrupt query results, only the DML status row. Alternative — engine counts consumed rows and ignores the provider's number: rejected; it is wrong for `Replace`-style semantics and hides provider bugs instead of surfacing them.

## Risks / Trade-offs

- [Provider partially writes then fails; the engine cannot roll back] → Honest contract, stated in the spec and on the interface doc: exactly-once delivery, no retry, commit-or-rollback before returning is the provider's job (identical to DataFusion's own `DataSink` contract). The in-repo test provider demonstrates the all-or-nothing pattern (buffer, then commit on successful drain).
- [`INSERT INTO t SELECT ... FROM t` (self-insert) runs the provider's scan and insert concurrently] → Providers are already required to be goroutine-safe for concurrent scans; the doc comment extends that to scan-during-insert and notes visibility of in-flight writes is provider-defined. No engine-side deadlock: scan pulls and the insert call sit on independent blocking-pool threads, and the bounded channel only ever blocks the pump against the consumer.
- [A provider that returns without draining or releasing the reader strands the pump] → Reader ownership transfers to Go and the trampoline releases it after `InsertInto` returns regardless of outcome (defer), which drops the receiver and unblocks/terminates the pump. A provider can't strand Rust resources by forgetting a release.
- [Long inserts occupy a blocking-pool thread for the whole statement] → Same class and mitigation as long scans (prior D6 trade-off); tokio's blocking pool scales, and correctness-first was already decided there.
- [Bounded-channel bridge adds a batch copy-free but hop-ful path (pump task + channel per statement)] → One-time per statement, not per batch crossing; noise next to a cgo call. If the bridge proves buggy the collect-then-send fallback (D4) preserves every observable contract.
- [`REPLACE INTO` may not parse in the default SQL dialect even though the mode is plumbed] → Verified `Append` and `Overwrite` are reachable from SQL; `Replace` passes through the same int and is exercised at the trampoline level in tests if the dialect refuses the syntax. No spec scenario depends on `REPLACE INTO` parsing.
- [Future DataFusion upgrades change `insert_into`/`InsertOp`] → The 54.1.0-verified facts in Context are re-checked on any DataFusion bump, same policy as the pushdown change's verified facts.
- [Discovered during implementation: if the pump task (D4) panics rather than polling a normal `Err` from the input stream, dropping `tx` looks identical, from the Go side, to a clean end-of-stream — Go could report a truncated insert as a success] → `write_all` awaits the pump's `JoinHandle` and treats a panicked join (not just a normal `Err`) as a failure, even when the Go call itself reported success. In practice this is a narrow residual case: every other Go-callback boundary in this codebase already converts a Go-side panic into a normal `Err` before it would reach the pump (`spawn_go_blocking` catches join panics and returns them as `Result::Err`), so triggering this specifically requires a raw panic inside DataFusion's own physical-plan execution — a pre-existing, general risk for any query, not one this change introduces. Not separately covered by a dedicated test (hard to trigger deterministically through the stub infrastructure without a contrived panic), but the fix is a few lines and directly observable in the code.

## Migration Plan

Purely additive. Existing providers implement neither new interface and keep byte-identical behavior; `INSERT` against them fails as it does today (with a clearer message). The `df_session_register_table` signature change is internal to the single link unit and rebuilt together (pushdown precedent). Rollback = revert the commits. No data or on-disk format concerns.

## Open Questions

- Channel capacity for the D4 bridge (a small constant; tuned during implementation, observable only as memory/latency, never as behavior).
- Whether the parquet/iceberg write follow-up lands as one change or two — does not affect this change's specs, approach, or tasks.
