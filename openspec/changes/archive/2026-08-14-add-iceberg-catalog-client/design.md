## Context

See proposal.md — Why. Current state of `providers/iceberg` (verified by reading `providers/iceberg/iceberg.go`, `pushdown.go`, `convert.go` on `main`, plus the uncommitted `wire-parquet-and-iceberg-pushdown` change that sits on top of it):

- `NewTableProvider(ctx, metadataLocation string)` builds a `*tableProvider{metadataLocation, schema}`. `Schema()` returns the cached schema. `Scan` calls an internal `loadTable(ctx, metadataLocation)` — which does `icebergio.LoadFSFunc(nil, metadataLocation)` + `table.NewFromLocation(ctx, ident, metadataLocation, fsysF, nil)` — fresh on every call (design D4 of `add-parquet-and-iceberg-table-providers`), then scans and bridges via `array.ReaderFromIter`.
- The in-flight `wire-parquet-and-iceberg-pushdown` change adds `ScanWithOptions(ctx, *datafusion.ScanOptions)` to the same `tableProvider`, which also starts from `loadTable(ctx, t.metadataLocation)`, then applies `table.WithRowFilter`/`WithSelectedFields`/`WithLimit` before scanning.
- Every entry point that needs a live `*table.Table` goes through the same one-line `loadTable` helper keyed on `t.metadataLocation`. That is the only place this change needs to generalize.

Verified facts about `iceberg-go` v0.6.0 (`go doc` and the module cache, not memory):

- `catalog.Catalog` (package `github.com/apache/iceberg-go/catalog`) is an interface with `LoadTable(ctx context.Context, identifier table.Identifier) (*table.Table, error)`, `CheckTableExists`, `CatalogType() catalog.Type`, and the rest of the create/list/namespace surface. `table.Identifier = []string` (`table/table.go:71`) — a namespace path followed by the table name, e.g. `[]string{"db", "orders"}` for `db.orders`.
- Five catalog implementations ship as **separate subpackages**, each independently importable and each registering itself via `init()` when imported: `catalog/rest` (`rest.NewCatalog(ctx, name, uri string, opts ...rest.Option) (*rest.Catalog, error)`), `catalog/hive` (Thrift client, pulls a generated `hive_metastore` package), `catalog/glue` (wraps `aws-sdk-go-v2`'s Glue client), `catalog/sql` (`sql.NewCatalog(name string, db *sql.DB, dialect, props)`, works over any `database/sql` driver via `bun`), and `catalog/hadoop` (a directory-convention catalog with no external service — already used by this package's own tests today, see below).
- `catalog/rest`'s `Option` surface (`catalog/rest/options.go`) covers `WithOAuthToken`, `WithCredential` (client-credentials flow), `WithAuthManager` (custom `AuthManager`), `WithSigV4`/`WithSigV4RegionSvc`, `WithTLSConfig`/`WithOAuthTLSConfig`, `WithHeaders`, `WithWarehouseLocation`, `WithPrefix`, `WithCustomTransport`, `WithAdditionalProps`, and more — a comprehensive, already-idiomatic options API for exactly the "auth/config for REST catalog" surface this change would otherwise have to invent.
- `providers/iceberg`'s own existing tests (`providers/iceberg/iceberg_test.go`) already build local test fixtures through `catalog/hadoop`'s `NewCatalog` + `CreateNamespace` + `CreateTable` + `Append`, purely as a fixture-generation tool — production code still uses `table.NewFromLocation`. `catalog/rest`'s own test suite (`catalog/rest/rest_test.go`) mocks the REST catalog protocol with `net/http/httptest.Server`, not a live service.
- `go.mod` importing `github.com/apache/iceberg-go` already pulls in the whole module at the Go-module level; individual subpackages (`catalog`, `catalog/rest`, `catalog/hadoop`, ...) add no new `go.mod` requires — only their own transitive imports (e.g. `catalog/rest` pulls `aws-sdk-go-v2` and `golang.org/x/oauth2`; `catalog/hive`/`catalog/glue`/`catalog/sql` pull heavier, service-specific dependencies) are only compiled into whichever package actually imports them.

## Goals / Non-Goals

**Goals:**

- Resolve and open an Iceberg table by namespace-qualified identifier through any `catalog.Catalog` implementation, with `providers/iceberg` itself importing only the `catalog` interface package — never a specific catalog client.
- Keep `NewTableProvider(ctx, metadataLocation)` byte-identical in behavior; it is not touched beyond internal refactoring that does not change its observable contract.
- Make the new construction path a first-class peer of the existing one for scanning: `Scan` and (once merged) `ScanWithOptions` work identically regardless of how the provider was constructed, with no duplicated scan/pushdown logic.
- Test and document the REST catalog client (`catalog/rest`) end to end, since it's the primary target.

**Non-Goals:**

- Wrapping, re-exposing, or reimplementing any catalog-specific auth/config surface (OAuth, SigV4, TLS, headers, ...) — `catalog/rest`'s `Option` API already covers this; callers use it directly.
- Testing or documenting Hive, Glue, SQL, or Hadoop catalog clients as first-class paths of this change. They work today through the same `catalog.Catalog` interface (`catalog/hadoop` is already exercised as a test-fixture tool), and nothing in this change's design is REST-specific, but exercising and documenting each is separate follow-up work, deliberately deferred to keep this change's testing surface to one real catalog implementation.
- Catalog write paths (create/drop table or namespace, commits) — read-only resolution only, matching the provider's existing read-only scope.
- The DataFusion-level `CatalogProvider` FFI mechanism (`datafusion`/`rust`) that would expose a whole catalog of tables to SQL's planner — a separate, parallel proposal. This change is entirely about how one `providers/iceberg` provider locates its one table.
- Object-store support beyond what `iceberg-go`'s table/catalog code already provides for a given catalog type.
- Snapshot/time-travel selection — still always the table's current snapshot, now simply re-resolved per scan for catalog-backed tables (see D2).

## Decisions

**D1 — New constructor `NewTableProviderFromCatalog(ctx context.Context, cat catalog.Catalog, identifier ...string) (datafusion.TableProvider, error)`, generic over any `catalog.Catalog`.**

```go
import (
    "github.com/apache/iceberg-go/catalog"
    "github.com/apache/iceberg-go/catalog/rest"
    icebergprovider "github.com/cedricziel/datafusion-golang/providers/iceberg"
)

cat, err := rest.NewCatalog(ctx, "prod", "https://iceberg.example.com/api", rest.WithOAuthToken(token))
table, err := icebergprovider.NewTableProviderFromCatalog(ctx, cat, "db", "orders")
```

Accepting the `catalog.Catalog` **interface** rather than a URI/config struct means `providers/iceberg` never imports `catalog/rest` (or any concrete catalog package) itself — the caller builds and configures the catalog client with whichever package they need, using that package's own idiomatic options API, and only *that* import pulls in that catalog's transitive dependencies. This directly extends the rationale of `add-parquet-and-iceberg-table-providers` design D1 (keep both formats' dependency footprints opt-in): a consumer who wants the REST catalog pays for `aws-sdk-go-v2`/`oauth2`; a consumer who never touches catalogs at all — or only uses the `metadata.json` path — pays for neither, and `providers/iceberg`'s own `go.mod`/import graph is unaffected either way, since these are subpackages of the already-required `iceberg-go` module.

Identifier is `...string` (namespace levels, then table name — mirroring `table.Identifier = []string` exactly, e.g. `"db", "orders"` or `"a", "b", "orders"` for a nested namespace) rather than a single dotted string. Alternative — accept `"db.table"` and split on `.`: rejected; Iceberg namespace/table name components are not guaranteed dot-free across every catalog implementation, so a split-on-dot parser would be ambiguous for nested namespaces or unusual names, whereas `...string` has zero ambiguity and matches `iceberg-go`'s own convention (`cat.LoadTable(ctx, []string{"default", "people"})`) used throughout its codebase and this package's own tests. A README/example snippet shows the common case (`strings.Split("db.orders", ".")...`) for callers who do have a flat, dot-free naming convention.

**D2 — Internal refactor: generalize the provider's table-loading step behind a small unexported abstraction; `Scan`/`ScanWithOptions` become loader-agnostic.**

Today `tableProvider` stores `metadataLocation string` and every scan path calls the free function `loadTable(ctx, t.metadataLocation)`. This change adds a second loading strategy (catalog + identifier) that must plug into the exact same scan/pushdown code without duplicating it. Shape:

```go
type tableProvider struct {
    schema *arrow.Schema
    load   func(ctx context.Context) (*table.Table, error)
    describe func() string // for error messages, e.g. "metadata.json at ..." or "catalog table db.orders"
}

func NewTableProvider(ctx context.Context, metadataLocation string) (datafusion.TableProvider, error) {
    load := func(ctx context.Context) (*table.Table, error) { return loadTableFromLocation(ctx, metadataLocation) }
    return newProvider(ctx, load, func() string { return metadataLocation })
}

func NewTableProviderFromCatalog(ctx context.Context, cat catalog.Catalog, identifier ...string) (datafusion.TableProvider, error) {
    load := func(ctx context.Context) (*table.Table, error) { return cat.LoadTable(ctx, table.Identifier(identifier)) }
    return newProvider(ctx, load, func() string { return "catalog table " + strings.Join(identifier, ".") })
}
```

`Scan` and `ScanWithOptions` (whichever lands first between this change and `wire-parquet-and-iceberg-pushdown`) call `t.load(ctx)` instead of `loadTable(ctx, t.metadataLocation)` — a one-line change at each call site, with no change to the pushdown/projection/filter-conversion logic that follows. This is the only touch point between the two changes; see "Interaction with wire-parquet-and-iceberg-pushdown" below. Alternative — a `tableSource` interface with a `Load(ctx) (*table.Table, error)` method instead of a closure: equivalent; a closure is chosen only because there's exactly one method and no implementation-specific state beyond what the closure already captures.

**D3 — Catalog-backed scans re-resolve per scan (design D4 of the original change, extended); this is a deliberate behavioral difference from the metadata.json path, not an inconsistency.**

The metadata.json path is pinned to one specific metadata file forever (Iceberg writes a new metadata file per commit; `NewFromLocation` never looks past the file it was given). The catalog path calls `cat.LoadTable` fresh on every scan, which returns whatever the catalog currently considers the table's live metadata pointer — so a commit made between two scans of the same catalog-backed provider becomes visible without reconstructing the provider. This falls directly out of D2's per-scan `load(ctx)` call and needs no extra code; it's called out explicitly (and specified — see specs delta) because it's a real, user-visible difference between the two construction paths that a caller migrating from one to the other should know about.

**D4 — No auth/config wrapping; `providers/iceberg` accepts only a constructed `catalog.Catalog`.**

Considered and rejected: a `RESTOptions` struct or a thin re-export of `rest.Option` inside `providers/iceberg` to spare callers an extra import. Rejected because (a) `catalog/rest`'s own `Option` API is already complete and idiomatic — duplicating it would be a maintenance burden that drifts on every `iceberg-go` bump, exactly the kind of shifting-internals risk `wire-parquet-and-iceberg-pushdown`'s design flags as a concern for its own dependency; (b) it would force `providers/iceberg` to import `catalog/rest` (and therefore `aws-sdk-go-v2`) unconditionally, reintroducing the dependency-footprint problem D1 exists to avoid; (c) `NewTableProviderFromCatalog`'s generic `catalog.Catalog` signature already gives every catalog implementation — including future ones — the identical integration path for free, with no per-catalog code in this package at all.

**D5 — Error wrapping distinguishes "not found" from other catalog errors only by message, not by a typed error.**

`catalog.Catalog.LoadTable` returns `catalog.ErrNoSuchTable`/`catalog.ErrNoSuchNamespace` (sentinel errors) on a missing table, and other errors (network, auth) unwrapped from whatever the concrete client returns. `NewTableProviderFromCatalog` wraps whatever `LoadTable` returns with `fmt.Errorf("iceberg: load catalog table %s: %w", ...)` (matching the existing `loadTable`'s wrapping style) and does not need to special-case `ErrNoSuchTable` — the spec only requires *a* Go error, not a specific type, and `errors.Is(err, catalog.ErrNoSuchTable)` already works through `%w` for a caller who wants to distinguish it. Alternative — define provider-specific sentinel errors: rejected, unnecessary indirection over sentinels `iceberg-go` already exports.

**D6 — Testing strategy: `catalog/rest` against an `httptest.Server` fake, table fixtures built via `catalog/hadoop` as today.**

No live REST catalog service is available or desirable in unit tests. `catalog/rest`'s own test suite (`catalog/rest/rest_test.go`) validates the client against `net/http/httptest.Server` handlers that implement just the REST catalog endpoints exercised (`GET /v1/config`, `GET /v1/namespaces/{ns}/tables/{table}`, returning a `LoadTableResult`-shaped JSON body with `metadata-location`/`metadata`). This change follows the same pattern: a small test-only REST catalog fake in `providers/iceberg`'s test package, serving a table created on disk via the existing `catalog/hadoop`-based fixture helper (`newIcebergFixture`, already in `iceberg_test.go`) — the fake's handler reads that table's real `metadata.json` off disk and returns it verbatim, so the client-side code under test (this change) is exercised against wire-format-correct REST responses without any external process. This mirrors, rather than duplicates, `iceberg-go`'s own test approach, and keeps the test dependency surface to `catalog/rest` + `net/http/httptest`, both already reachable with no new `go.mod` entries.

## Interaction with wire-parquet-and-iceberg-pushdown

The two changes touch the same package on independent axes and compose without conflict:

- **This change**: how a `*table.Table` handle is obtained (metadata.json vs. catalog + identifier) — touches construction and the single `load`/`loadTable` call at the top of each scan path (D2).
- **`wire-parquet-and-iceberg-pushdown`**: what happens *after* a `*table.Table` handle is obtained (projection/filter/limit conversion into `table.ScanOption`s) — touches everything from `tbl.Scan(...)` onward.
- Because D2 isolates the loading step behind `t.load(ctx)`, `ScanWithOptions`'s body (filter conversion, `WithSelectedFields`, `WithLimit`, output reordering) is untouched by this change regardless of merge order: if `wire-parquet-and-iceberg-pushdown` lands first, this change's D2 refactor replaces its one `loadTable(ctx, t.metadataLocation)` call site inside `ScanWithOptions` with `t.load(ctx)`; if this change lands first, `wire-parquet-and-iceberg-pushdown` is implemented directly against `t.load(ctx)` and never needs `t.metadataLocation` at all.
- Net effect once both are merged (in either order): a catalog-backed provider gets scan pushdown automatically, with zero catalog-specific pushdown code — `ScanWithOptions` doesn't know or care whether `t.load` came from a file path or a catalog client.
- Neither change modifies the other's spec requirements; the specs delta here only touches construction/schema/scan-freshness/resource-lifecycle requirements, none of which the pushdown change's delta (projection/filter/limit requirements) overlaps.

## Risks / Trade-offs

- [The D2 refactor touches shared code (`loadTable`/`Scan`/`ScanWithOptions`) that `wire-parquet-and-iceberg-pushdown` is concurrently, independently modifying — a merge-order collision is possible even though the changes are logically orthogonal] → Keep the refactor to the smallest possible diff (one call-site substitution per scan path, per D2); land whichever change merges first, then rebase the other's one-line call sites onto the new `t.load(ctx)` shape — a mechanical, low-risk conflict resolution.
- [A caller supplies a `catalog.Catalog` implementation this change never tested (Hive/Glue/SQL/Hadoop) and hits an edge case `NewTableProviderFromCatalog` didn't anticipate] → Acceptable; the function only calls the interface's documented `LoadTable` method, and the interface contract (not a REST-specific one) is what's being relied on. Explicitly a non-goal to test beyond REST (see Non-Goals); worth a follow-up change per catalog type if demand appears.
- [Per-scan catalog re-resolution (D3) adds one network round trip per scan, versus zero for the metadata.json path] → Deliberate and matches the semantics a catalog-backed table should have (see current data); acceptable overhead, consistent with the original change's D4 accepting one extra metadata read per scan for the same reason.
- [`catalog.Catalog`'s `LoadTable` signature or `table.Identifier` shape shifts in a future `iceberg-go` bump] → Small surface (one method call), already used elsewhere in the module by every catalog implementation, low churn risk; verified facts here are re-checked on any version bump, matching the existing project convention.

## Migration Plan

Additive only — a new constructor alongside the existing one, plus an internal refactor with no observable behavior change for `NewTableProvider`. No rollback concerns beyond reverting the commits. Existing callers of `NewTableProvider(ctx, metadataLocation)` are unaffected.

## Open Questions

None — the catalog set, the generic `catalog.Catalog` signature, the identifier shape, the D2 refactor's shared-code touch points, and the test strategy are all resolved above based on verified `iceberg-go` v0.6.0 APIs and the current state of both this package and the concurrent pushdown change.
