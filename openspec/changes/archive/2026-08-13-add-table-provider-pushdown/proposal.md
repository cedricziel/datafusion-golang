# Proposal: add-table-provider-pushdown

## Why

Every SQL query against a Go-registered table today performs a full, unprojected, unfiltered scan: the Rust adapter reports `Unsupported` for every filter, and the projection DataFusion computes is applied per batch in Rust *after* the complete Go scan. That was an explicit Non-Goal of `add-table-provider-and-scalar-udf`, deferred until real backing stores existed that could benefit. They now exist: `providers/parquet` and `providers/iceberg` both sit on APIs with genuine pruning hooks (Parquet row-group selection and column statistics via `pqarrow.FileReader.GetRecordReader(ctx, colIndices, rowGroups)`; Iceberg manifest-level data-file pruning via `iceberg-go` scan row filters and field selection), but the scan callback gives them nothing to prune with — a `WHERE` clause on a partitioned Iceberg table still reads every data file, and a two-column `SELECT` on a wide Parquet file still decodes and ships every column across the FFI boundary.

## What Changes

- A Go table provider can opt into scan pushdown by implementing a new optional interface (working name `PushdownTableProvider`) with a `ScanWithOptions(ctx, *ScanOptions)` method alongside the existing `TableProvider`. Providers that implement only `TableProvider` keep today's behavior, bit for bit.
- `ScanOptions` carries three things per scan:
  - **Projection**: the column indices (into the registered schema) the query needs. A pushdown provider MUST honor the projection exactly — return exactly those columns, in that order — and the engine validates the returned stream's schema against the expected projected schema. Projected-away columns never cross the FFI boundary.
  - **Filters**: the query's conjunctive predicates, restricted to a bounded, closed predicate AST (column refs, typed literals, `=`/`!=`/`<`/`<=`/`>`/`>=` comparisons, `AND`/`OR`/`NOT`, `IS [NOT] NULL`, `[NOT] BETWEEN`, `[NOT] IN` (list)). Filters are strictly advisory: the engine reports every pushed filter to DataFusion as `Inexact`, so DataFusion re-applies every filter after the scan. A provider may use them to skip data (row groups, data files), partially apply them, or ignore them entirely — results are identical either way.
  - **Limit**: DataFusion's pushed-down fetch limit when one exists, also advisory (the engine always keeps its own limit operator above the scan).
- Predicates that don't fit the bounded AST (arbitrary functions, casts, subqueries, regex, arithmetic, ...) are reported `Unsupported` and evaluated by DataFusion after the scan, exactly as today.
- The engine never reports `Exact` in this change: no user-supplied Go code is trusted to fully evaluate a filter, so correctness never depends on what a provider does with the pushed filters.
- Rust-side classification of which conjuncts are representable happens in pure Rust during planning (in `supports_filters_pushdown`), with **no new FFI callback into Go**: the provider's opt-in is a boolean declared at registration time, and the filter payload rides on the existing scan callback.
- The `go_table_scan` trampoline and `df_session_register_table` FFI signatures are extended (projection indices, serialized filters, limit, pushdown flag). This is an internal ABI shared by one link unit, not a public surface; the public Go `TableProvider` interface is unchanged (non-breaking).
- `providers/parquet` and `providers/iceberg` are NOT updated to consume the new capability in this change (explicit Non-Goal; natural follow-up work). An in-repo test provider exercises the new interface end to end.

## Capabilities

### New Capabilities

None. The predicate AST and scan options exist solely as the contract between the engine and a registered table provider; today there is no second consumer that would justify a standalone cross-cutting capability (e.g. `expression-pushdown`). If a later change reuses the expression representation elsewhere (catalog pruning, delete filters), extracting a shared capability can happen then.

### Modified Capabilities

- `table-provider`: the "Full-table scan" requirement is scoped to providers that don't opt into pushdown, and new requirements are added covering the opt-in pushdown scan (projection honored exactly and validated; filters advisory with the engine re-applying them; limit advisory; unsupported predicate shapes withheld; error handling and session lifecycle unchanged).

## Impact

- **Code**: `datafusion/table.go` (optional `PushdownTableProvider` interface, `ScanOptions`, registration-time capability detection, extended `go_table_scan` trampoline); new `datafusion/expr.go` (bounded predicate AST types + deserialization); `rust/datafusion-c-abi/src/table.rs` (`supports_filters_pushdown` classification, filter serialization, projected-schema validation, no more Rust-side re-projection for pushdown providers); `include/datafusion_go.h` (extended signatures); tests in `datafusion/table_test.go` and Rust `table.rs` tests.
- **Dependencies**: none new on the Go side (stdlib `encoding/json` for the wire format). Rust side: optionally `serde`/`serde_json` for filter serialization, or a small hand-written serializer — decided in design.md. Explicitly avoided: `datafusion-proto` and `datafusion-substrait` (both exist at 54.1.0 but are rejected in design.md D2).
- **Downstream**: unlocks real row-group pruning in `providers/parquet` and manifest pruning in `providers/iceberg` as follow-up changes; establishes the expression-crossing convention any later pushdown surface (e.g. `LIKE`, `limit`-only sources, aggregate pushdown) would extend.
