## 1. Internal refactor: loader abstraction (design D2)

- [x] 1.1 Check whether `wire-parquet-and-iceberg-pushdown` has merged to `main` yet; if it has, `ScanWithOptions` already exists and this refactor must update its `loadTable` call site too, not just `Scan`'s
- [x] 1.2 Rename the free function `loadTable(ctx, metadataLocation)` to `loadTableFromLocation` (or fold into a constructor-local closure) and add `load func(context.Context) (*table.Table, error)` plus a `describe func() string` field to `tableProvider`, replacing the stored `metadataLocation string` field
- [x] 1.3 Update `NewTableProvider` to build its `load`/`describe` closures over `metadataLocation`, preserving its exact current error messages and behavior
- [x] 1.4 Update `Scan` (and `ScanWithOptions`, if present) to call `t.load(ctx)` instead of `loadTable(ctx, t.metadataLocation)`; update any error-message string interpolation to use `t.describe()`
- [x] 1.5 Run the full existing `providers/iceberg` test suite unchanged and confirm it still passes — this task must not change `NewTableProvider`'s observable behavior at all

## 2. Catalog constructor (design D1, D3, D5)

- [x] 2.1 Write failing tests first: `NewTableProviderFromCatalog(ctx, cat, identifier...)` against a `catalog.Catalog` fake/stub returning a real `*table.Table` (built via the existing `catalog/hadoop` fixture helper) succeeds and exposes the expected schema
- [x] 2.2 Implement `NewTableProviderFromCatalog(ctx context.Context, cat catalog.Catalog, identifier ...string) (datafusion.TableProvider, error)`: calls `cat.LoadTable(ctx, table.Identifier(identifier))`, derives the Arrow schema via `table.SchemaToArrowSchema` (same call `NewTableProvider` already makes), builds `load`/`describe` closures per D2/D3 that call `cat.LoadTable` fresh each time
- [x] 2.3 Add tests: table not found (`catalog.ErrNoSuchTable`/`ErrNoSuchNamespace`) surfaces as a Go error; a stub catalog returning a generic error (simulating connectivity/auth failure) surfaces as a Go error, not a panic
- [x] 2.4 Add a test asserting `errors.Is(err, catalog.ErrNoSuchTable)` still works through the wrapped error (design D5)

## 3. Catalog-backed scan behavior (design D2, D3)

- [x] 3.1 Add a test: scanning a catalog-backed provider returns the full current-snapshot contents, spanning multiple data files, matching the existing metadata.json-path scenarios
- [x] 3.2 Add the scan-freshness test from the spec delta: scan a catalog-backed provider, commit a new snapshot to the same table through the catalog (e.g. another `Append` via the fixture's catalog handle), scan the same provider again, and assert the second scan reflects the new snapshot while the first scan's already-returned reader is unaffected
- [x] 3.3 Add a resource-lifecycle/concurrency test mirroring the existing metadata.json-path test: concurrent scans of the same catalog-backed provider under `go test -race` each return correct, independent results
- [x] 3.4 If `wire-parquet-and-iceberg-pushdown` has merged by this point, add one smoke test confirming `ScanWithOptions` (projection/filter/limit) works unmodified against a catalog-backed provider — proving the composition described in design.md's "Interaction with wire-parquet-and-iceberg-pushdown" holds in practice

## 4. REST catalog integration test (design D6)

- [x] 4.1 Build a minimal test-only REST catalog fake: an `httptest.Server` implementing `GET /v1/config` and `GET /v1/namespaces/{ns}/tables/{table}`, backed by a table created on disk via the existing `catalog/hadoop`-based fixture helper (`newIcebergFixture` in `iceberg_test.go`), returning that table's real on-disk metadata verbatim
- [x] 4.2 Write an integration test: `rest.NewCatalog(ctx, name, fakeServerURL)` (no auth options needed against the fake) + `NewTableProviderFromCatalog(ctx, cat, "default", tableName)`, register on a real `SessionContext`, and query it via SQL end to end
- [x] 4.3 Confirm the fake need not implement OAuth/SigV4/TLS paths (the fake serves plain HTTP with no auth configured) — if `catalog/rest`'s `NewCatalog` requires more of the protocol than `/v1/config` + table-load to complete client construction, extend the fake minimally and note it in a code comment

## 5. Example, docs, and wrap-up

- [x] 5.1 Extend `examples/parquet-iceberg` (or add a new example) demonstrating `NewTableProviderFromCatalog` against a REST catalog, alongside the existing metadata.json-based example
- [x] 5.2 Update README's `providers/iceberg` section: document `NewTableProviderFromCatalog`, show the REST catalog usage snippet, and revise the "Scope boundaries" paragraph — Iceberg catalog services are no longer categorically out of scope; note REST is the tested/documented path and Hive/Glue/SQL/Hadoop work through the same interface but are untested by this change
- [x] 5.3 Run `make lint && make format`, ensure `go test ./... -race` passes across all packages (including the refactored and new `providers/iceberg` code) and no pre-existing tests regress, ensure clean tree
- [x] 5.4 `openspec validate "add-iceberg-catalog-client" --strict` and fix any issues
- [x] 5.5 Commit series (semantic commits: loader refactor, catalog constructor, catalog-backed scan tests, REST integration test, example/docs)
