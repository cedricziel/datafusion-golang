# Tasks: add-table-provider-pushdown

## 1. Predicate AST and wire format

- [x] 1.1 Write failing Go tests for the predicate AST decoder in `datafusion/expr.go`: JSON documents for each node kind (`Compare` with all six operators, `IsNull`/`IsNotNull`, `Between`, `InList`, `And`/`Or`/`Not`) and each supported literal kind (bool, int widths, float32/64, string, base64 binary, date32/64, timestamp with unit + optional tz) decode into the expected typed values; malformed/unknown-kind documents return errors (design D2, D3, D7)
- [x] 1.2 Implement `datafusion/expr.go`: exported AST types (`Expr`, `Column{Name, Index}`, `Literal`, `Compare`, `IsNull`, `Between`, `InList`, `And`, `Or`, `Not`) with comparisons constrained to column-vs-literal at the type level, plus the `encoding/json` decoder (design D2, D3)
- [x] 1.3 Write failing Rust tests for the recognizer + serializer in `rust/datafusion-c-abi`: representable exprs (`col op lit`, `lit op col` normalized to column-left, `IS [NOT] NULL`, `[NOT] BETWEEN`, `[NOT] IN`, `AND`/`OR`/`NOT` nesting) are accepted and serialize to the documented JSON; unrepresentable exprs (casts, function calls, `LIKE`, arithmetic, unknown columns, null/decimal literals) are rejected (design D7)
- [x] 1.4 Implement the shared recognizer and the `serde_json`-based serializer (add `serde`/`serde_json` to `Cargo.toml`); one recognizer function used by both classification and serialization so scan-time serialization is total by construction (design D2, D3, D7)
- [x] 1.5 Add a cross-language round-trip test: Rust-serialized documents for a representative predicate set are decoded by the Go parser into the expected AST (golden JSON fixtures shared by both test suites)

## 2. FFI surface

- [x] 2.1 Extend `include/datafusion_go.h`: `df_session_register_table` gains `uint8_t supports_pushdown`; `go_table_scan` gains `const int32_t *projection`/`intptr_t projection_len` (-1 = none), `const char *filters_json` (NULL = none), `int64_t limit` (-1 = none); document ownership (filters string owned by caller for the duration of the call) and sentinel conventions (design D5)
- [x] 2.2 Update the Rust FFI declarations and `df_session_register_table` to accept and store the pushdown flag on `GoTableProvider`; update the Rust-side test stub trampolines for the new `go_table_scan` signature, recording the projection/filters/limit each scan receives (design D4, D5)
- [x] 2.3 Update the Go trampoline `go_table_scan` in `datafusion/table.go`: dispatch on the provider's type — `PushdownTableProvider` ⇒ decode filters JSON (loud error on decode failure), build `ScanOptions`, call `ScanWithOptions`; plain `TableProvider` ⇒ call `Scan(ctx)` ignoring the extra parameters (design D5, D7)

## 3. Rust adapter: planning and scan

- [x] 3.1 Write failing Rust test: a pushdown-flagged stub table queried with `WHERE id > 2 AND length(name) = 3` reports `Inexact` for the comparison and `Unsupported` for the function conjunct (assert via the filters that reach the stub's scan), while a non-flagged table reports all-`Unsupported` and receives no filters (design D1, D4)
- [x] 3.2 Implement `GoTableProvider::supports_filters_pushdown`: pure-Rust classification via the recognizer when `supports_pushdown` is set, all-`Unsupported` otherwise; no FFI call during planning (design D1, D4)
- [x] 3.3 Implement the pushdown scan path in `GoTableExec`: serialize filters, pass projection indices and limit through `go_table_scan`, expect the projected stream schema back, validate it (names, types, order; metadata ignored) with a descriptive error on mismatch, and skip the Rust-side per-batch `RecordBatch::project`; keep the legacy path byte-identical for non-flagged handles, `spawn_blocking` unchanged on both (design D5, D6, prior D6)
- [x] 3.4 Add Rust tests: pushdown stub honoring projection returns correct projected results with no re-projection; stub returning a wrong-schema stream fails the query with the mismatch error and the session stays usable; `Projection == None` expects the full schema; limit hint arrives for filterless `LIMIT n` and is `-1` when a `WHERE` is present (design D6, D8)

## 4. Go public API and end-to-end tests

- [x] 4.1 Write failing Go tests in `datafusion/table_test.go` with an in-repo pushdown test provider (records the `ScanOptions` it receives): `SELECT name FROM t WHERE id > 2` delivers the projection and the decoded `id > 2` filter and returns correct rows; a provider that ignores all filters returns results identical to the equivalent non-pushdown provider; a provider that prunes non-matching batches using the filter returns correct results; `SELECT * FROM t LIMIT 3` delivers the limit hint and returns 3 rows whether the provider truncates or not (spec: all pushdown scenarios)
- [x] 4.2 Implement `PushdownTableProvider`, `ScanOptions`, and registration-time capability detection (type assertion feeding the `supports_pushdown` flag) in `datafusion/table.go` (design D4)
- [x] 4.3 Implement and test the `ProjectReader(reader array.RecordReader, indices []int) array.RecordReader` helper, and use it in the test provider (design D6)
- [x] 4.4 Add Go tests for the failure modes: pushdown provider returning a non-projected stream surfaces a schema-mismatch query error with the session usable afterwards; `ScanWithOptions` returning an error surfaces as a query error; concurrent queries against one pushdown provider pass under `go test -race`
- [x] 4.5 Verify non-pushdown providers are untouched end to end: existing `datafusion`, `providers/parquet`, `providers/iceberg`, and example tests pass unmodified

## 5. Docs and wrap-up

- [x] 5.1 Document the pushdown contract on the Go types: filters advisory ("may omit only rows that cannot satisfy the filters" — the MUST NOT in exactly those terms), projection exact, limit hint semantics including `-1` whenever a `WHERE` is pushed (design D1, D8; spec: filter/limit requirements)
- [x] 5.2 Update README: pushdown opt-in interface, what is and isn't pushed down, and that `providers/parquet`/`providers/iceberg` pruning is planned follow-up work
- [x] 5.3 Run `make lint && make format`, ensure `go test ./... -race` and the Rust test suite pass, clean tree
- [x] 5.4 Commit series (semantic commits: predicate AST + wire format, FFI surface, Rust adapter, Go API + tests, docs)
