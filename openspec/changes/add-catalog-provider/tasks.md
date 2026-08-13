## 1. Shared FFI groundwork

- [x] 1.1 Extend `include/datafusion_go.h` with the new C ABI: `df_session_register_catalog`, and `extern` declarations for the six new Go-exported trampoline symbols (`go_catalog_schema_names`, `go_catalog_schema_lookup`, `go_catalog_release`, `go_schema_table_names`, `go_schema_table_lookup`, `go_schema_release`), documenting handle-ownership and the JSON-name-list convention (design D2, D5)
- [x] 1.2 Add a Rust helper to run a Go callback via `tokio::task::block_in_place` for sync trait methods (design D4), alongside the existing `spawn_blocking`-based helper reused for the async `table()` lookup
- [x] 1.3 Add JSON array-of-strings encode/decode helpers on both sides (Rust: `serde_json` into `Vec<String>`; Go: `encoding/json` from `[]string`), reusing the existing malloc/`free` string-ownership convention (design D5)

## 2. Catalog and schema providers — Rust side

- [x] 2.1 Implement `GoCatalogProvider` wrapping a `cgo.Handle`-derived `usize`; `schema_names()` and `schema()` call `go_catalog_schema_names`/`go_catalog_schema_lookup` via `block_in_place` (design D1, D4); `schema()` returns `None` (not an error) when the Go call reports "not found" (design D7)
- [x] 2.2 Implement `GoSchemaProvider` wrapping a `cgo.Handle`-derived `usize`; `table_names()` calls `go_schema_table_names` via `block_in_place`; `table_exist()` is derived from `table_names()` in pure Rust, no FFI call (design D2)
- [x] 2.3 Implement `SchemaProvider::table()` (async): call `go_schema_table_lookup` via the existing `spawn_blocking` helper; on a "not found" result return `Ok(None)` (design D7); on a found result, construct a `GoTableProvider` via the existing `GoTableProvider::try_new(handle, supports_pushdown)` — no new table-scan code path (design D1, D2)
- [x] 2.4 Implement `Drop for GoCatalogProvider` calling `go_catalog_release`, `Drop for GoSchemaProvider` calling `go_schema_release`
- [x] 2.5 Implement `df_session_register_catalog`: reject duplicate non-default names (design D6) before touching `CatalogProviderList::register_catalog`; do not special-case the default catalog name — construct `GoCatalogProvider` and register it via the session's `SessionContext::register_catalog` unconditionally (overwrite-and-return-previous for the default name is the accepted behavior), `catch_unwind`-wrapped per the existing convention
- [x] 2.6 Rust unit test: a fake Go-side catalog (stubbed trampoline functions) registered and queried via `SELECT * FROM catalog.schema.table` through the raw C ABI, covering: successful resolution, duplicate-registration-rejected, registering under the default catalog name replaces it (previously-registered tables/functions become unreachable), unknown-schema, unknown-table, schema-names-error, table-lookup-error, and a catalog-discovered table exercising both full-scan and pushdown paths (reusing the existing stub table infrastructure from `table.rs`'s tests)

## 3. Catalog and schema providers — Go side

- [x] 3.1 Write failing Go test: implement a `datafusion.CatalogProvider`/`datafusion.SchemaProvider` backed by an in-memory map of tables, `RegisterCatalog`, `SQL("SELECT * FROM cat.sch.t")`, assert results match
- [x] 3.2 Define `datafusion.CatalogProvider` (`SchemaNames`, `Schema`) and `datafusion.SchemaProvider` (`TableNames`, `Table`) interfaces per design D1, and `SessionContext.RegisterCatalog(name string, catalog CatalogProvider) error`
- [x] 3.3 Implement the `//export`ed trampolines (`goCatalogSchemaNames`, `goCatalogSchemaLookup`, `goCatalogRelease`, `goSchemaTableNames`, `goSchemaTableLookup`, `goSchemaRelease`) resolving `cgo.Handle`s, encoding/decoding JSON name lists, minting a fresh `cgo.Handle` for each `Schema`/`Table` lookup result, recovering panics into the error out-param
- [x] 3.4 Make test 3.1 pass; add tests for: duplicate catalog name error, registering under the default catalog name succeeds and shadows previously-registered tables/functions, unknown schema/table produce the engine's standard not-found error (not a Go error), schema-names/table-lookup error surfaces and session stays usable, a table added to the backing map after registration becomes queryable without re-registering, catalog and its resolved schema/table not invoked after session `Close()`
- [x] 3.5 Add a test where `Table` returns a `PushdownTableProvider`, asserting projection/filters/limit are delivered exactly as for a directly-`RegisterTable`d pushdown provider (reusing existing pushdown test fixtures where practical)
- [x] 3.6 Add concurrency test: multiple goroutines querying `catalog.schema.table` against the same registered catalog concurrently under `go test -race`

## 4. Example, docs, and wrap-up

- [x] 4.1 Extend `examples/extend` (or add a new example) demonstrating a registered catalog whose schema lists tables discovered from something simple (e.g. a directory listing or an in-memory map that changes between two queries), queried via `catalog.schema.table`
- [x] 4.2 Update README: extensibility section covering `RegisterCatalog`, the `CatalogProvider`/`SchemaProvider` interfaces, how a catalog-discovered table composes with the existing `TableProvider`/`PushdownTableProvider` contracts, the goroutine-safety contract, and current limitations (read-only catalogs, no caching, registering under the default catalog name replaces it); update the Roadmap bullet to remove "catalog providers"
- [x] 4.3 Run `make lint && make format`, ensure `go test ./... -race` and `cargo test` both pass, ensure clean tree
- [x] 4.4 Commit series (semantic commits: FFI groundwork, Rust catalog/schema providers, Go catalog/schema providers, example/docs)
