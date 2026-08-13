## Why

`providers/iceberg` can only open an Iceberg table by its exact local `metadata.json` path — the README states this as an explicit scope boundary ("no Iceberg catalog services (REST/Glue/Hive)"). Real Iceberg tables are looked up by a namespace-qualified identifier (`db.table`) through a catalog service, not a file path a caller has to already know. `iceberg-go` v0.6.0 — already pinned in `go.mod` — ships a `catalog.Catalog` interface plus working REST, Hive, Glue, SQL, and Hadoop catalog client implementations, so resolving a table through a catalog is Go-side wiring against an existing library, not new FFI or Rust work.

## What Changes

- New constructor `providers/iceberg.NewTableProviderFromCatalog(ctx, cat catalog.Catalog, identifier ...string)`: resolves and opens a table through a caller-supplied, already-configured `catalog.Catalog`, instead of a `metadata.json` path. `providers/iceberg` depends only on the small `github.com/apache/iceberg-go/catalog` interface package — never on a specific catalog implementation (`catalog/rest`, `catalog/hive`, `catalog/glue`, `catalog/sql`, `catalog/hadoop`) — so its own dependency footprint doesn't grow regardless of which catalog a caller chooses. Callers construct the catalog client themselves (e.g. `rest.NewCatalog(ctx, name, uri, opts...)`) and hand it to the new constructor; `providers/iceberg` neither wraps nor re-exposes any catalog-specific auth/config options.
- The existing `NewTableProvider(ctx, metadataLocation string)` constructor (the direct `metadata.json` path) is unchanged and continues to work exactly as before.
- Internally, both constructors produce the same provider type: a scan re-resolves the table (via the catalog or the metadata location, matching how it was constructed) each time it runs, so any scan pushdown support already implemented for the metadata.json path (`ScanWithOptions`, in the concurrent `wire-parquet-and-iceberg-pushdown` change) applies to catalog-backed tables automatically, with no duplicated pushdown logic.
- Catalog-backed scans reflect the table's latest committed state at scan time (the catalog is re-queried per scan), unlike the metadata.json path, which is pinned to whatever snapshot that specific metadata file recorded.
- REST is the primary, tested catalog client for this change (`github.com/apache/iceberg-go/catalog/rest`); the example and README document it. Hive, Glue, SQL, and Hadoop catalog clients work through the same `catalog.Catalog` interface but are not exercised or documented by this change — they remain available to a caller today, unchanged.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `iceberg-table-provider`: adds a second construction path — resolving a table through a caller-supplied Iceberg `catalog.Catalog` client and namespace-qualified identifier — alongside the existing direct `metadata.json` path. Scan, schema, error, and resource-lifecycle requirements extend to cover catalog resolution; no existing requirement's meaning for the metadata.json path changes.

## Impact

- **Code**: `providers/iceberg/` only — a new constructor and its supporting internals (`catalog.go` or similar), an example update, and tests. No changes to `datafusion/`, `rust/`, or `include/`.
- **Dependencies**: none added to `go.mod` — `github.com/apache/iceberg-go` is already required; `catalog` is a subpackage of that same module. Tests and the example additionally import `github.com/apache/iceberg-go/catalog/rest` (and, for local test fixtures, `catalog/hadoop`, as the package's tests already do), which is already available transitively via the existing `iceberg-go` requirement.
- **Scope boundaries**: this change does not touch the DataFusion-level `CatalogProvider` FFI mechanism (a separate, parallel proposal at the `datafusion`/`rust` layer) — it is entirely about how `providers/iceberg` resolves one table's location, not about exposing a catalog of tables to DataFusion's SQL planner. It also does not add object-store support beyond what `iceberg-go`'s catalog/table implementations already provide, and does not change snapshot-selection behavior (still always the current snapshot, now re-resolved per scan for catalog-backed tables).
