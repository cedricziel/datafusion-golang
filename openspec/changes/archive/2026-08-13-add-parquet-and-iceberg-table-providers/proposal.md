# Proposal: add-parquet-and-iceberg-table-providers

## Why

The engine can already query any Go-implemented `TableProvider` (schema + scan), but every provider so far has been hand-written for tests. Parquet and Iceberg are the two data formats users actually want to query, and both have mature, pure-Go implementations (`arrow-go`'s `parquet`/`pqarrow` packages, and `apache/iceberg-go`). Wrapping them as `TableProvider` implementations keeps the project's stated direction — as much logic in Go as possible, Rust staying a thin execution layer — and needs zero changes to the FFI boundary, since `RegisterTable` and the callback machinery already exist.

## What Changes

- New package `providers/parquet`: `NewTableProvider(path string) (datafusion.TableProvider, error)` opens a local Parquet file, exposing its Arrow schema and reading all row groups as record batches on `Scan`.
- New package `providers/iceberg`: `NewTableProvider(ctx context.Context, metadataLocation string) (datafusion.TableProvider, error)` opens a local-filesystem-backed Iceberg table directly from its `metadata.json` location (no catalog service required), exposing its current schema and reading its current snapshot's data files as record batches on `Scan`.
- Both are separate, independently-importable packages (not added to the core `datafusion` package) so consumers who only need core SQL execution and custom providers don't pull in Parquet/Iceberg's dependency footprint.
- File handles and other per-scan resources are released deterministically when a returned `RecordReader` is released or fully consumed, including under concurrent scans of the same provider.

## Capabilities

### New Capabilities

- `parquet-table-provider`: Opening a local Parquet file as a queryable `TableProvider` — schema derivation, full-file scan across row groups, error and resource-lifecycle behavior.
- `iceberg-table-provider`: Opening a local-filesystem-backed Iceberg table (via its metadata location, no catalog service) as a queryable `TableProvider` — schema derivation, current-snapshot multi-file scan, error and resource-lifecycle behavior.

### Modified Capabilities

_None — these are new clients of the existing `table-provider` engine contract (`RegisterTable`, schema-before-scan, errors-as-Go-errors) established in `add-table-provider-and-scalar-udf`. That contract does not change._

## Impact

- **Code**: New packages `providers/parquet/` and `providers/iceberg/` (each with its own tests and, where useful, an example). No changes to `datafusion/`, `rust/`, or `include/`.
- **Dependencies**: `github.com/apache/iceberg-go` (new, pinned to v0.6.0); `github.com/apache/arrow-go/v18`'s `parquet`/`pqarrow` subpackages (already an indirect dependency via the core module, now used directly). `go.mod` pins `arrow-go/v18` to v18.7.0 explicitly to avoid version drift against iceberg-go's own (older) pin.
- **Scope boundaries**: local filesystem only for both — no S3/GCS/Azure object-store access, no Iceberg catalog services (REST/Glue/Hive), no snapshot/time-travel selection, no filter or projection pushdown. These are explicit non-goals for this change, consistent with the table-provider contract's existing no-pushdown stance.
