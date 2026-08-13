## Why

The engine now delivers scan pushdown (`PushdownTableProvider` / `ScanOptions`, from `add-table-provider-pushdown`), but neither bundled provider uses it: `providers/parquet` and `providers/iceberg` still decode every column of every row and ship it across the FFI boundary, only for DataFusion to project and filter most of it away. For a `WHERE` clause on a partitioned or sorted dataset and a narrow `SELECT` list, both formats already carry the metadata needed to skip that work — Parquet per-row-group column statistics and bloom filters, Iceberg manifest partition/column stats — and both libraries expose it (verified against `arrow-go` v18.7.0 and `iceberg-go` v0.6.0).

## What Changes

- `providers/parquet` implements `datafusion.PushdownTableProvider`:
  - Projection is honored exactly by passing the projected columns' Parquet leaf indices to `pqarrow.FileReader.GetRecordReader`, so projected-away columns are never decoded.
  - Pushed filters drive row-group skipping: a row group is read only if its per-column min/max statistics and null counts cannot prove the predicate unsatisfiable there. Bloom filters additionally prune row groups for equality and IN predicates on supported column types.
  - The limit hint truncates the scan's output once at least that many rows have been produced.
- `providers/iceberg` implements `datafusion.PushdownTableProvider`:
  - Pushed filters are converted from the engine's bounded predicate AST to `iceberg.BooleanExpression` and passed via `table.WithRowFilter`, which unlocks iceberg-go's own manifest-level pruning, per-data-file metrics pruning, Parquet row-group/bloom pruning, and row-level filtering.
  - Projection is passed via `table.WithSelectedFields` (pruning file reads to the needed columns) and reordered to the requested column order.
  - The limit hint is passed via `table.WithLimit`.
- A dedicated, adversarially-tested pruning/conversion layer in each package: a statistics-based skip evaluator (Parquet) and an AST-to-`BooleanExpression` converter (Iceberg), each with direct unit tests independent of the engine/FFI integration tests.
- Non-pushdown behavior (`Scan`) is unchanged; results of every query are identical with and without pruning, per the advisory-filter contract.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `parquet-table-provider`: the provider becomes pushdown-capable — exact projection, statistics/bloom-based row-group skipping under the one-directional "skip only what provably cannot match" rule, and advisory limit truncation.
- `iceberg-table-provider`: the provider becomes pushdown-capable — filter conversion into iceberg-go's scan (manifest/data-file/row-group pruning), selected-field projection with exact column order, and advisory limit pass-through.

## Impact

- Code: `providers/parquet/parquet.go` (+ new pruning evaluator and tests), `providers/iceberg/iceberg.go` (+ new expression converter and tests). No changes to the core `datafusion` package, the FFI surface, or the Rust side.
- Dependencies: none added; uses APIs already present in the pinned `arrow-go` v18.7.0 (`parquet/metadata` statistics and bloom filters) and `iceberg-go` v0.6.0 (`table.ScanOption`, `iceberg` expressions).
- Contracts: must satisfy the existing `table-provider` spec exactly — projection is a hard contract; filters are advisory and may only omit rows that cannot satisfy them.
