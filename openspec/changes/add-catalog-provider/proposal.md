# Proposal: add-catalog-provider

## Why

Every table the engine can query today must be registered one at a time, by name, before any SQL runs (`RegisterTable`/`RegisterScalarUDF`). SQL can therefore only ever address a flat, pre-declared set of unqualified table names — there is no way to expose a catalog whose schemas and tables are discovered dynamically (e.g. backed by a directory listing, a metadata service, or any store where the table set isn't known until query time), and no way for SQL to address a table as `catalog.schema.table`. DataFusion's own catalog API (`CatalogProviderList`/`CatalogProvider`/`SchemaProvider`) already supports exactly this, but nothing exposes it across the FFI boundary to Go. This change adds that FFI capability, reusing the callback-trampoline conventions established by `add-table-provider-and-scalar-udf` and, within it, the table-provider mechanism itself.

## What Changes

- New Go interfaces `datafusion.CatalogProvider` (`SchemaNames`, `Schema`) and `datafusion.SchemaProvider` (`TableNames`, `Table`); a session can `RegisterCatalog(name string, catalog CatalogProvider) error` making `catalog.schema.table` queryable via SQL.
- `SchemaProvider.Table` returns the *existing* `datafusion.TableProvider` (or `PushdownTableProvider`) interface — a catalog-discovered table is registered and scanned through the exact same Rust wrapper (`GoTableProvider`) and trampolines (`go_table_schema`/`go_table_scan`/`go_table_release`) as a table passed to `RegisterTable` today. No new table-scan FFI is introduced.
- New Rust-side FFI machinery: a Go-backed `CatalogProvider` trait impl and a Go-backed `SchemaProvider` trait impl, dispatching through new fixed Go-exported trampoline symbols (`go_catalog_schema_names`, `go_catalog_schema_lookup`, `go_catalog_release`, `go_schema_table_names`, `go_schema_table_lookup`, `go_schema_release`), each carrying an opaque `cgo.Handle`-derived handle, following the same idioms as the existing table/UDF trampolines.
- Schema and table names cross Go→Rust as a single JSON array-of-strings string, using the same allocate-in-Go/free-in-Rust ownership convention already used for error strings, with zero new dependencies (`serde_json`/`encoding/json` are already used for pushdown filters).
- The default catalog name (`datafusion`) is not special-cased: registering a Go catalog under it replaces the default catalog outright, and any tables/functions previously registered via `RegisterTable`/`RegisterScalarUDF` become unreachable through SQL — the same overwrite-and-return-previous behavior DataFusion's own `register_catalog` already has.
- Schema and table lookups are evaluated lazily, per query, against whatever the Go catalog currently reports — nothing is cached at registration time, unlike `RegisterTable`'s eager schema fetch. Handles for discovered schemas/tables are correspondingly short-lived (roughly one query's table resolution), not held for the session's lifetime.
- Panics inside the new Go callbacks and Go-side errors they return must not unwind into Rust; they surface as query planning/execution errors, matching the existing table/UDF contract.
- No schema/table mutation: `register_schema`/`deregister_schema`/`register_table`/`deregister_table` are left unimplemented (DataFusion's "not implemented" default) — a Go-registered catalog is read-only from SQL's perspective in this change (no `CREATE SCHEMA`/`CREATE TABLE` against it).

## Capabilities

### New Capabilities

- `catalog-provider`: Registering a Go-implemented catalog on a session and querying `catalog.schema.table` via SQL, including dynamic schema/table listing, lazy per-query table lookup, and composability with the existing table-provider mechanism.

### Modified Capabilities

- `sql-execution`: `SessionContext` gains `RegisterCatalog`; closing a session must also release the engine's reference to every registered catalog, extending the existing close/release requirement.

## Impact

- **Code**: `datafusion/catalog.go` (new — `CatalogProvider`/`SchemaProvider` interfaces, `RegisterCatalog`, trampolines), `datafusion/cgo.go` extensions if needed, `rust/datafusion-c-abi/src/catalog.rs` (new — `GoCatalogProvider`/`GoSchemaProvider` trait impls), `rust/datafusion-c-abi/src/lib.rs` (`df_session_register_catalog`), `include/datafusion_go.h` additions (registration function, new callback trampoline declarations).
- **Dependencies**: none added on either side — reuses `serde_json`/`encoding/json` (already present for pushdown filters) and the existing table-provider machinery.
- **Downstream**: establishes the mechanism a future Iceberg/Glue/Hive-style catalog client (a separate, independently proposed change) could plug into as a `datafusion.CatalogProvider` implementation; this change does not implement or depend on any such client.
