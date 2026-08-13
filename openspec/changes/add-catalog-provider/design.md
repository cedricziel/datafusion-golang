## Context

See proposal.md — Why. `add-table-provider-and-scalar-udf` established the callback-trampoline pattern this change reuses unchanged: fixed Go-exported `//export` symbols (not vtables) resolved at final link time, opaque `cgo.Handle`-derived `uintptr_t` handles with explicit `_release` trampolines, `catch_unwind` at every Rust entry point, a heap-C-string error convention, and every Go callback running off the calling goroutine's or a `spawn_blocking` thread. `add-table-provider-pushdown` added `ScanOptions`/`PushdownTableProvider` on top of that. This change adds a third registration kind — a catalog — that sits *above* the existing table-provider mechanism rather than beside it: a catalog's job is to hand back objects that are themselves table providers, discovered lazily instead of declared up front.

DataFusion 54.1.0's catalog traits (`datafusion-catalog` crate, verified against the pinned version's source) are:

```rust
trait CatalogProviderList: Any + Debug + Sync + Send {
    fn register_catalog(&self, name: String, catalog: Arc<dyn CatalogProvider>) -> Option<Arc<dyn CatalogProvider>>;
    fn catalog_names(&self) -> Vec<String>;
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>>;
}

trait CatalogProvider: Any + Debug + Sync + Send {
    fn schema_names(&self) -> Vec<String>;                          // sync
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>; // sync
    // register_schema/deregister_schema default to a "not implemented" error
}

trait SchemaProvider: Any + Debug + Sync + Send {
    fn table_names(&self) -> Vec<String>;                             // sync
    fn table_exist(&self, name: &str) -> bool;                        // sync
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>>; // async
    // register_table/deregister_table default to a "not implemented" error
}
```

`SessionContext::register_catalog` calls straight into `CatalogProviderList::register_catalog` on the session's own list. `schema_names`/`schema`/`table_names` are **sync** trait methods called during query planning (itself running inside the shared tokio runtime); only `SchemaProvider::table` is `async`.

## Goals / Non-Goals

**Goals:**

- Let a Go program register a catalog whose schemas and tables DataFusion discovers by asking the Go implementation, at query time, not at registration time.
- Make SQL able to address `catalog.schema.table`.
- Reuse the existing table-provider mechanism for the leaf: a table a catalog hands back is queried exactly like a table passed to `RegisterTable`, through the same Rust wrapper and the same `go_table_*` trampolines — this change adds no new table-scan FFI.
- Keep every new Go callback panic- and error-safe, per the existing convention.

**Non-Goals:**

- Any concrete catalog client (Iceberg REST/Glue/Hive or otherwise). This change is the mechanism only; a client is separate, independently proposed work that may consume this mechanism later.
- Schema or table mutation through the catalog (`CREATE SCHEMA`, `CREATE TABLE ... ` against a Go-registered catalog, `DROP`, etc.) — `register_schema`/`deregister_schema`/`register_table`/`deregister_table` are left at DataFusion's default "not implemented" behavior. A Go catalog is read-only from SQL's perspective in this change.
- Caching or pinning resolved schemas/tables across queries — every query resolves `catalog.schema.table` afresh (see D3, and the lifetime risk it documents).
- `CatalogProviderList` itself (registering a whole alternate catalog *list* implementation, replacing DataFusion's default in-memory one). Only `CatalogProvider`/`SchemaProvider` are exposed to Go; the session's own `CatalogProviderList` (DataFusion's built-in `MemoryCatalogProviderList`) still owns the mapping from catalog name to `Arc<dyn CatalogProvider>`, exactly as `SessionContext::register_catalog` already provides.
- Table type introspection shortcuts (`SchemaProvider::table_type`) — left at its default (`table(name).await.map(|t| t.table_type())`), since it exists purely as an optional fast path for `information_schema.tables` and a Go catalog's `table()` call is already required to be reasonably cheap (D3).

## Decisions

**D1 — `CatalogProvider`/`SchemaProvider` are exposed as two more Go interfaces, not one combined "catalog" interface, and `SchemaProvider.Table` returns the existing `datafusion.TableProvider`.**
Mirroring DataFusion's own three-level model (catalog → schema → table) keeps the Go API legible and lets `Table` return the *exact* interface `RegisterTable` already accepts — a program can hand the same `TableProvider` implementation to either registration path. Concretely:

```go
type CatalogProvider interface {
    SchemaNames(ctx context.Context) ([]string, error)
    Schema(ctx context.Context, name string) (SchemaProvider, bool, error)
}

type SchemaProvider interface {
    TableNames(ctx context.Context) ([]string, error)
    Table(ctx context.Context, name string) (TableProvider, bool, error)
}

func (s *SessionContext) RegisterCatalog(name string, catalog CatalogProvider) error
```

`Schema`/`Table` return `(value, found bool, error)` rather than a nil-means-not-found convention, so "not found" (which DataFusion surfaces as its own standard error) is never ambiguous with "the Go program returned a nil interface by mistake." Alternative considered: a single `CatalogProvider` interface exposing `Table(ctx, schema, table string)` directly, skipping the schema level — rejected because it can't represent `schema_names()`/`table_names()` (needed for `information_schema` and schema-level "not found" errors) without an awkward compound-key listing API, and it throws away the natural mapping to DataFusion's own trait split.

**D2 — New fixed trampoline symbols per kind, reusing the table trampolines unchanged for the leaf.**
Two new kinds, six new symbols total: `go_catalog_schema_names`, `go_catalog_schema_lookup`, `go_catalog_release` (catalog kind); `go_schema_table_names`, `go_schema_table_lookup`, `go_schema_release` (schema kind). `go_schema_table_lookup` returns a `uintptr_t` handle to a `TableProvider` plus a `supports_pushdown` flag — the same shape `df_session_register_table` already takes — which the Rust side feeds straight into the existing `GoTableProvider::try_new`. `table_exist` gets no trampoline of its own: it is implemented in Rust as `self.table_names().contains(name)`, since DataFusion's own default `table_type()` implementation already accepts the cost of calling `table()` for a similar convenience method, and adding a seventh symbol for a derivable boolean is not worth the surface area (revisit only if profiling shows repeated `table_names()` calls matter).

**D3 — Nothing is cached at registration; every query resolves the catalog afresh, mirroring DataFusion's own sync/async trait split.**
`df_session_register_catalog` does not call into Go at all (unlike `df_session_register_table`'s eager `go_table_schema` fetch) — a catalog's entire purpose is to report content not known up front, so there is nothing meaningful to cache. Each `CatalogProvider::schema_names`/`schema` call and each `SchemaProvider::table_names`/`table` call is a fresh Go round-trip. Consequently, handles for `SchemaProvider` and catalog-discovered `TableProvider` objects are **not** session-lifetime like a `RegisterTable` handle: each is minted fresh by a Go-side lookup call and released (`go_schema_release`/`go_table_release`) as soon as the Rust `Arc` wrapping it is dropped, which may happen once per query or more often (e.g. once per table reference within a query, depending on how DataFusion's planner calls `schema()`/`table()`). This is a deliberate simplicity-first choice; see Risks for the cost, and see Open Questions for the caching alternative this rules out for now.

**D4 — Sync trait methods use `tokio::task::block_in_place`; only the async `table()` lookup uses the existing `spawn_blocking` helper.**
`CatalogProvider::schema_names`/`schema` and `SchemaProvider::table_names` are *sync* trait methods, called during query planning from within the shared multi-thread tokio runtime (walking-skeleton D5) — they cannot `.await` a `spawn_blocking` future the way `TableProvider::scan`/`ScalarUDFImpl::invoke_with_args` do, because there is no `async fn` to await inside. Calling the Go trampoline directly and synchronously from these methods would block whatever tokio worker thread is running the current query's planning for the duration of the Go call. `tokio::task::block_in_place` is the correct tool here: it tells the multi-thread runtime's scheduler that the current worker thread is about to block, so it can hand off other ready tasks to a different worker thread first — valid only because the runtime is always `Builder::new_multi_thread` (walking-skeleton D5), which this project already guarantees. `SchemaProvider::table` *is* `async`, so it reuses the existing `spawn_blocking`-based helper unchanged, exactly like `TableProvider::scan`. Alternative considered: making every sync method call Go inline with no mitigation — rejected as a straightforward runtime-starvation risk under concurrent catalog-heavy queries, the same class of risk `add-table-provider-and-scalar-udf`'s D6 already identified and mitigated for the table/UDF case.

**D5 — Schema/table name lists cross as one JSON array-of-strings string, using the existing error-string ownership convention.**
Go allocates a JSON array (`["a","b"]`) via `C.CString` (malloc) into an out-param; Rust parses it with `serde_json` and frees it with `free` — the exact ownership shape `include/datafusion_go.h` already documents for error strings, and the exact serialization stack (`serde_json` / `encoding/json`) already used for pushdown filter predicates (`rust/datafusion-c-abi/src/pushdown.rs`, `datafusion/expr.go`). No new dependency, no new ownership pattern to document. Alternative considered: a `char**`/count array of individually-owned C strings — rejected as needing a second allocation-and-free convention (array of pointers, each freed separately) for no benefit at the sizes involved (schema/table name lists, not per-row data).

**D6 — The default catalog name is not special-cased; registering under it replaces it.**
DataFusion's default `CatalogProviderList` (used by `SessionContext::new()`) always contains a catalog named `datafusion` with a `public` schema, and `RegisterTable`/`RegisterScalarUDF` register directly into that catalog's default schema and the session's function registry respectively. `df_session_register_catalog` does not check the name against the engine's default-catalog name — it calls `CatalogProviderList::register_catalog` directly, which overwrites-and-returns-previous per DataFusion's own semantics. Registering a Go catalog named `datafusion` therefore replaces the default catalog outright, and any tables/functions a program previously registered via `RegisterTable`/`RegisterScalarUDF` become unreachable through SQL — an accepted, documented consequence rather than a case to guard against; this project doesn't add compatibility protections for existing registrations. Duplicate non-default names are still rejected the same way D8 already rejects duplicate table/function names (checked via `CatalogProviderList::catalog(name)` before registering) — that check guards against a program accidentally double-registering the same catalog name, a distinct concern from default-catalog protection.

**D7 — "Not found" at the schema or table level is DataFusion's own error, not a Go-authored one.**
`Schema`/`Table` returning `found = false` becomes `None` from the Rust trait method, which DataFusion's own planner turns into its standard schema-not-found / table-not-found error — the same error shape an unqualified `SELECT * FROM missing_table` already produces today. A Go implementation therefore never needs to construct its own "not found" error message; only genuine failures (I/O errors, timeouts, malformed responses from whatever backs the catalog) become the Go `error` return value, which surfaces as a query error per the existing error convention (walking-skeleton D4).

**D8 — A Go error from `schema_names`/`schema`/`table_names` (all infallible per the pinned trait shapes) is raised as a Rust panic, not returned; two different things catch it depending on which DataFusion code path called in.**
`CatalogProvider::schema_names`/`schema` and `SchemaProvider::table_names` return `Vec<String>`/`Option<Arc<dyn SchemaProvider>>` — no `Result` — so a Go-reported failure has nowhere to go except a panic (discovered during implementation; not addressed by D7, which only covers the "not found" case for the two methods that do return `Result`-shaped values). Empirically (Rust unit tests, `catalog.rs`), this panic is caught by two different mechanisms depending on the caller:
- A direct `catalog.schema.table` reference resolves via a synchronous, planning-time call to `schema()` — the panic propagates as a normal Rust unwind up through `ctx.sql(...).await`, caught by the FFI entry point's own `catch_unwind` (walking-skeleton D4) and converted to a query-error string via `panic_message`.
- `information_schema.tables`/`.schemata` (the only DataFusion-internal caller of `schema_names()`/`table_names()` — see below) builds its virtual tables inside a Tokio-spawned task; a panic there is caught by Tokio's own task-join machinery first and arrives as an ordinary `DataFusionError::External(JoinError)`, already a clean `Result::Err` by the time it would reach any `catch_unwind`.

Both satisfy the spec's "abort the query, surface a Go error, session stays usable" requirement; which one occurs is not something this project controls or needs to — it isn't something a Go implementation needs to know about, either way.

Implementing this surfaced a **pre-existing, unrelated bug**: the FFI entry points' `panic_message(&payload)` call, where `payload: Box<dyn Any + Send>`, was unsizing the `Box` itself into `dyn Any` rather than dereferencing into the boxed panic value — so every `catch_unwind` site in `lib.rs` had always produced `"panic: <non-string payload>"` for any real panic, regardless of its actual message. Nothing previously exercised a genuine panic through any of these four call sites, so it went unnoticed until this change's tests did. Fixed by passing `payload.as_ref()` instead of `&payload` at all four sites — a one-line, unrelated correctness fix, included in this change since it was found while building it.

Also discovered: `schema_names()`/`table_names()` are **only** called by DataFusion when `information_schema` is enabled (`SessionConfig::with_information_schema(true)`) — a direct qualified reference resolves via `schema()`/`table()` point lookups and never enumerates. `df_session_new` does not enable `information_schema` (it uses `SessionContext::new()`'s defaults) and this change does not add a way to turn it on, so as of this change, `CatalogProvider.SchemaNames`/`SchemaProvider.TableNames` are implemented and correctly wired, but **not reachable from a Go program via SQL** — only `Schema`/`Table` (the point-lookup paths) are exercised in practice today. This is a real, documented limitation, not a defect: the methods exist because the DataFusion trait requires them and because a future change enabling `information_schema` should not need any FFI changes to work.

## Risks / Trade-offs

- [Per-query (or more frequent) `Arc` churn for `SchemaProvider`/catalog-discovered `TableProvider` handles (D3) means repeated Go round-trips — including a fresh `go_table_schema` fetch — for the same `catalog.schema.table` reference across queries, unlike a `RegisterTable` table's one-time schema fetch] → acceptable for this phase; correctness and simplicity come first, and a Go implementation is free to memoize internally (e.g. cache its own schema/table metadata) since nothing prevents that. Revisit with Rust-side resolved-schema caching only if profiling on real usage shows it matters (see Open Questions).
- [`block_in_place` (D4) still occupies a worker thread for the duration of the Go call, just without blocking the rest of the runtime — a Go catalog implementation that is slow or blocks indefinitely during `SchemaNames`/`Schema` degrades planning latency for whatever query triggered it] → matches the existing "no cancellation yet" limitation already documented for `Scan`/scalar UDFs; the same expectation (implementations must terminate promptly) extends here, not a new risk class.
- [A catalog-discovered `TableProvider` is looked up fresh per resolution, so a provider that is expensive to construct (e.g. opens a connection in its constructor) pays that cost repeatedly] → the Go program controls its own `TableProvider` implementation and can defer expensive work into `Scan`/`ScanWithOptions` rather than construction, exactly as any `TableProvider` should already do; not a new constraint this change introduces.
- [Registering a catalog under the default catalog name (D6) silently drops previously-registered tables/functions from SQL reachability] → deliberate; this project accepts breaking changes over compatibility guards (see project conventions), and the behavior is documented rather than hidden. A program that wants to avoid this simply picks a non-default catalog name.

## Migration Plan

Additive to `add-table-provider-and-scalar-udf`/`add-table-provider-pushdown`; no existing behavior changes except the `sql-execution` delta (`Close` now also releases registered catalogs — a strengthening, not a breaking change, since no catalogs could be registered before this change). No rollback concerns beyond reverting the commits.

## Open Questions

- Whether to add Rust-side caching of resolved `SchemaProvider`/`TableProvider` objects within a single query (or across queries, with an invalidation story) to cut down on the per-resolution Go round-trips D3 accepts — deferred until real usage shows the extra round-trips matter; does not change this change's specs or task breakdown, since D3's behavior (always ask Go) remains correct either way, just not maximally efficient.
- Whether `SchemaProvider::owner_name` (an optional trait method, defaults to `None`, surfaced only in `information_schema.schemata`) is worth exposing to Go now or left at its default — a small addition either way, resolved during coding rather than design.
