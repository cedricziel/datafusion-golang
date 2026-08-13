# Design: add-table-provider-pushdown

## Context

See proposal.md — Why. The current machinery (see `openspec/changes/archive/2026-08-13-add-table-provider-and-scalar-udf/design.md`, decisions D1–D8): fixed `extern "C"` trampoline symbols resolved at static-link time (D1), `cgo.Handle` opaque tokens (D2), schema fetched once at registration (D3), streaming scan via a real Arrow C Stream (D4), every Rust→Go call on `spawn_blocking` (D6), close-time safety inherited from the session's RWMutex draining (D7), Go-side duplicate-name check (D8). This change extends that machinery; references like "prior D6" below point at that document.

Verified facts about the pinned `datafusion = "54.1.0"` (from the vendored sources, not memory):

- `TableProvider::supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>>` is a **sync** trait method. The enum variants are `Unsupported` / `Inexact` / `Exact` (`datafusion-expr-54.1.0/src/table_source.rs:37`).
- The `push_down_filter` optimizer rule (`datafusion-optimizer-54.1.0/src/push_down_filter.rs:1144-1210`) splits the `WHERE` predicate into conjuncts via `split_conjunction`, excludes volatile and subquery-containing conjuncts, and calls `supports_filters_pushdown` once with the remaining conjuncts. Every non-`Unsupported` conjunct is added to `TableScan.filters` (and later arrives at `scan()`); every non-`Exact` conjunct is **also kept in a `Filter` node above the scan**. So: `Inexact` ⇒ DataFusion re-applies the filter after the scan (our correctness safety net); `Exact` ⇒ DataFusion removes it and trusts the provider completely.
- `scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>)` is the signature our adapter already implements; `filters` receives exactly the conjuncts reported non-`Unsupported`. (54.1.0 also has a default-implemented `scan_with_args`; it delegates to `scan`, so implementing `scan` remains sufficient.)
- The `push_down_limit` rule always re-wraps the plan in a `Limit` node even when it pushes `fetch` into the scan (`push_down_limit.rs:223-238`), so a pushed limit is a pure hint. A `fetch` never crosses a retained `Filter` node, so limit hints reach the scan only when the query has no (or only `Exact`) pushed filters.
- Relevant `Expr` variants for predicates: `Column`, `Literal`, `BinaryExpr{left, op, right}` with `Operator::{Eq, NotEq, Lt, LtEq, Gt, GtEq, And, Or, ...}`, `Not`, `IsNull`/`IsNotNull`, `Between{expr, negated, low, high}`, `InList{expr, list, negated}`, `Like`, `Cast`, function calls, and many more. DataFusion's simplifier and cast-unwrapping rules run before `push_down_filter`, so common predicates arrive normalized as `column op literal`.
- `datafusion-proto` and `datafusion-substrait` both exist on crates.io at 54.1.0 (checked against the index), so both serialization alternatives in D2 are version-viable and are rejected on other grounds.

Go-side consumers this design must serve: `providers/parquet` prunes via `pqarrow.FileReader.GetRecordReader(ctx, colIndices, rowGroups)` — it needs column indices and per-column comparison predicates to select row groups from Parquet column statistics; `providers/iceberg` prunes via `iceberg-go` scan options (row filter expressions, selected fields) — it needs column names and comparison predicates.

## Goals / Non-Goals

**Goals:**

- Deliver projection, a bounded filter subset, and the limit hint to opted-in Go providers at scan time, with zero behavior change for providers that don't opt in.
- Keep correctness independent of provider behavior: the engine must produce right answers even if a provider ignores or half-applies every pushed filter.
- No new FFI callback into Go; extend the existing scan trampoline only (smallest consistent extension of prior D1/D6 machinery).
- Give provider authors an expression type they can pattern-match into Parquet row-group stats checks or `iceberg-go` filter expressions in a few lines.

**Non-Goals:**

- `Exact` filter pushdown (trusting a provider to fully evaluate a filter, letting DataFusion drop its own re-filter). Deferred until a concrete provider demonstrates the re-filter cost matters; the FFI shape reserves nothing for it and would gain a registration-time capability flag if it ever lands.
- Updating `providers/parquet` / `providers/iceberg` to actually prune (follow-up changes; this change ships the capability plus an in-repo test provider exercising it).
- General `Expr` interchange: `LIKE`/regex, arithmetic, function calls, casts surviving to the provider, `IS DISTINCT FROM`, subqueries. The classifier reports all of these `Unsupported`.
- Statistics reporting from Go providers (`TableProvider::statistics`), aggregate/TopK pushdown, and multi-partition scans.

## Decisions

**D1 — Pushed filters are always reported `Inexact`; the engine never reports `Exact`.**
For an opted-in provider, `GoTableProvider::supports_filters_pushdown` classifies each conjunct: representable in the bounded AST (D3) ⇒ `Inexact`, otherwise `Unsupported`. Because DataFusion keeps a `Filter` above the scan for every `Inexact` conjunct (verified, Context), the provider may do anything short of dropping rows that match the predicate — including nothing — and results are unchanged. This makes filter pushdown purely an optimization channel, never a correctness dependency on user Go code. Alternative — per-filter `Unsupported`/`Inexact`/`Exact` declared by the Go provider: rejected because it requires a planning-time FFI round-trip (see D4), because `Exact` from arbitrary user code turns a provider bug into silently wrong query results, and because for the motivating use cases (row-group/manifest pruning) `Inexact` is semantically what pruning *is* — surviving row groups still contain non-matching rows. Declaring `Unsupported` per-filter from Go would buy nothing: an ignored `Inexact` filter and an `Unsupported` filter produce identical plans (filter applied post-scan either way).

**D2 — Filters cross the FFI boundary as a hand-rolled, closed predicate AST — not `datafusion-proto`, not Substrait.**
The central decision. Alternatives, all version-viable at 54.1.0:

- *`datafusion-proto`* (protobuf serialization of DataFusion `Expr`): rejected. The Go side would need Go types generated from DataFusion's `.proto` files, which are not published as a Go module — we would vendor and regenerate them each DataFusion upgrade, coupling our public Go API to DataFusion's explicitly-unstable internal encoding. Worse, it hands provider authors the *full* generality of `Expr` (window functions, subqueries, ...), which they cannot act on for pruning and would have to defensively ignore.
- *Substrait* (`datafusion-substrait` crate Rust-side; `substrait-io/substrait-go/v8` is already a transitive Go dependency via `arrow-go`'s `compute/exprs` and `iceberg-go`): rejected for this change. Rust cost: the `substrait` + `prost` dependency tree compiled into the staticlib. Go cost is the real problem: a Substrait `ExtendedExpression` references functions through extension-URI anchors (e.g. resolve anchor → `functions_comparison.yaml` → `gt`) and columns through flat field indices — every provider author would need a Substrait interpreter just to recover "`id > 2`". Substrait's strength is fidelity across independently-versioned systems; our two sides are one link unit compiled together, so that strength buys nothing here. If a future change needs full expression fidelity (e.g. passing arbitrary predicates to a remote engine), Substrait is the escalation path and can coexist with this AST.
- *Bounded hand-rolled AST* (chosen): a closed set of node types — column reference (name **and** index into the registered schema, so Parquet-style consumers get indices and Iceberg-style consumers get names for free), typed literal, comparison (`=`, `!=`, `<`, `<=`, `>`, `>=`) constrained *at the type level* to column-vs-literal, `IS [NOT] NULL`, `[NOT] BETWEEN` (literal bounds), `[NOT] IN` (literal list), and `AND`/`OR`/`NOT` composition. The Rust classifier (D7) only admits expressions this AST can represent, so serialization is total by construction. Provider code pattern-matches directly: a `Compare{Column, Op, Literal}` maps onto a row-group min/max check or an `iceberg.GreaterThan(ref, lit)` in one line. Cost: one serializer (Rust) and one parser (Go), both small, both in-tree, no version skew possible.

Supported literal kinds (bounded, matching what column statistics can prune on): boolean, all integer widths, float32/64, UTF-8 strings, binary (base64 in the wire format), `Date32`/`Date64`, and `Timestamp` (with unit and optional timezone). Null literals, decimals, and nested types are not admitted in this change (classifier ⇒ `Unsupported`); decimals are the most likely first extension.

**D3 — Wire format is a single JSON document per scan, crossing as one `const char*`.**
The filter list serializes to a JSON array (top-level elements are implicit `AND`, mirroring DataFusion's conjunct list) of tagged expression objects. Rationale: one C string needs no new FFI struct types or ownership protocol beyond the existing malloc/free convention; it is human-readable in logs and test failures; Go parses it with stdlib `encoding/json` (zero new Go dependencies). Volume is bytes-to-kilobytes *per scan* (not per batch), so parse cost is noise next to a cgo crossing. Rust side uses `serde`/`serde_json` (new dev-visible dependency, ubiquitous and small) rather than a hand-written writer, because hand-writing JSON invites string-escaping bugs for exactly the values (string literals) most likely to contain quotes. Alternatives: a C struct tree (manual cross-language ownership of a recursive structure — the most error-prone option on offer); a custom binary format (unreadable, no measurable win at these sizes); per-literal Arrow arrays (an `FFI_ArrowArray` per literal is wildly heavier than the value it carries).

**D4 — Opt-in is a registration-time boolean; planning needs no call into Go.**
Go side: a provider opts in by implementing an optional interface (Go's standard capability pattern, like `io.WriterTo`):

```go
type ScanOptions struct {
    Projection []int  // indices into the registered schema; nil ⇒ all columns
    Filters    []Expr // implicit AND; advisory (engine re-applies); may be empty
    Limit      int64  // advisory fetch hint; -1 ⇒ none
}

type PushdownTableProvider interface {
    TableProvider
    ScanWithOptions(ctx context.Context, opts *ScanOptions) (array.RecordReader, error)
}
```

`RegisterTable` type-asserts for `PushdownTableProvider` and passes a `supports_pushdown` flag through `df_session_register_table`. `GoTableProvider` stores it; `supports_filters_pushdown` classifies conjuncts in **pure Rust** (D7) when the flag is set, and returns all-`Unsupported` (today's behavior) when it isn't. This matters doubly: `supports_filters_pushdown` is a *sync* trait method invoked during logical optimization on a tokio worker thread — a callback into Go there could neither use prior D6's `spawn_blocking` (no `await` in a sync method) nor safely block the worker inline. Keeping planning FFI-free sidesteps the problem entirely. Alternative — a new `go_table_supports_pushdown` trampoline consulted per query: rejected for the sync-context problem above, for growing the fixed symbol set without need, and because D1 already removed any value a per-filter Go-side verdict could add.

**D5 — The existing `go_table_scan` symbol is extended in place; the Go trampoline dispatches between the two provider shapes.**

```c
char *df_session_register_table(df_session_t session, const char *name,
                                uintptr_t handle, uint8_t supports_pushdown);

extern void go_table_scan(uintptr_t handle,
                          const int32_t *projection, intptr_t projection_len, /* -1 ⇒ no projection */
                          const char *filters_json,                          /* NULL ⇒ no filters */
                          int64_t limit,                                     /* -1 ⇒ no limit */
                          struct ArrowArrayStream *out_stream, char **error_out);
```

Both sides live in one link unit (prior D1), so changing a signature is an ordinary recompile, not an ABI migration. The Go trampoline dispatches: provider implements `PushdownTableProvider` ⇒ build `ScanOptions`, call `ScanWithOptions`; otherwise call `Scan(ctx)` and ignore the extra parameters (Rust passes null/-1 for non-opted-in providers anyway, since their filters were all `Unsupported` and projection stays Rust-side). Rust `GoTableExec` splits accordingly: non-pushdown handle ⇒ exactly today's behavior (full-schema stream, per-batch `RecordBatch::project`); pushdown handle ⇒ pass projection/filters/limit through, expect the projected schema back, and perform **no** Rust-side re-projection. `spawn_blocking` isolation for the scan call and every stream pull is unchanged (prior D6); filter serialization is pure Rust and runs before the callback. Alternative — a second symbol `go_table_scan_with_options` alongside the old one: rejected; two symbols means two Rust call paths and a dead parameter set on every provider forever, for no compatibility benefit inside a single link unit.

**D6 — Projection is a hard contract (validated), filters are advisory; not the reverse.**
Projection is mechanically trivial for any provider to honor exactly — selecting columns from an Arrow record is a few lines, and a `datafusion.ProjectReader(reader, indices)` helper ships with this change so backends without native column selection comply in one call. Making it exact is what keeps projected-away columns from ever crossing the FFI boundary (the main projection win). Rust validates the imported stream's schema against the expected projected schema (field count, names, types; metadata ignored) and fails the query with a descriptive error on mismatch — fail-fast on buggy providers instead of silently wrong columns. `Projection == nil` (DataFusion passed `None`) means the full registered schema is expected. Filters, by contrast, are *not* mechanically trivial (statistics, expression evaluation), so they stay advisory with the engine's re-filter as the safety net (D1). Alternative — advisory projection with Rust detecting from the returned schema whether the provider projected: rejected; detection is ambiguous when the projection is the identity or selects same-typed columns, and it silently forfeits the FFI-traffic win whenever a provider forgets.

**D7 — The Rust classifier is conservative and shared between planning and scan.**
One recognizer function decides "representable in the D2 AST": `Column op Literal` and `Literal op Column` (mirrored operators normalized so Go always sees the column on the left) for the six comparison operators; `IsNull`/`IsNotNull`, `Between`, `InList`, `Not` over representable operands with column/literal leaves; `BinaryExpr` with `And`/`Or` over representable children. Everything else — `Cast` (even around a literal), functions, `Like`, arithmetic, `IS TRUE`-family, `IS DISTINCT FROM`, columns not found in the registered schema — is `Unsupported`. Conservatism is free: a false negative only means DataFusion filters post-scan, exactly today's behavior. `supports_filters_pushdown` uses the recognizer to classify; `scan` serializes the filters it receives (all previously classified `Inexact`, per the optimizer contract verified in Context) with the same recognizer underneath, so scan-time serialization cannot encounter an unrepresentable expression. On the Go side a parse failure is a loud scan error, not a silent drop: both sides ship in one binary, so a decode mismatch is a bug to surface, not an interop condition to tolerate.

**D8 — The limit hint passes through as-is, trusted for nothing.**
Rust forwards `scan`'s `limit` (as `-1`/value); providers may truncate at it or ignore it. Safe because DataFusion always keeps its own `Limit` operator above the scan (verified, Context). Note the interaction documented there: a limit hint reaches the scan only for queries whose pushed filters are absent (or `Exact`, which we never report) — so with a `WHERE` clause, providers should expect `Limit == -1`. Included anyway because it rides the same struct for free and benefits filterless `LIMIT n` queries (e.g. previews) immediately.

## Risks / Trade-offs

- [A provider misuses an advisory filter and omits rows that *match* it] → The engine's re-filter can remove extra rows but cannot resurrect omitted ones; this failure is undetectable, same class as a provider returning wrong data. Mitigation: the `ScanOptions.Filters` doc comment and the spec state the MUST NOT in exactly these terms ("may omit only rows that cannot satisfy the filters"), and the follow-up provider changes get correctness tests comparing pruned vs. unpruned results.
- [Schema validation for pushdown providers is too strict (nullability or metadata differences fail valid streams)] → Validate names, types, and order; ignore field metadata. Nullability mismatches are decided during implementation with tests either way; when in doubt, accept (a false accept degrades to Arrow-level errors downstream, a false reject breaks working providers).
- [`Inexact` blocks limit pushdown past filters and keeps a re-filter cost on every pushed predicate] → Accepted cost of not trusting user code (D1); revisit `Exact` only with a motivating provider and a benchmark.
- [Two scan entry points (`Scan` / `ScanWithOptions`) could drift behaviorally] → Single dispatch point in the trampoline; the pushdown path is tested against the same scenarios as the legacy path plus the pushdown-specific ones.
- [Future DataFusion upgrades change what expression shapes reach `supports_filters_pushdown`] → The conservative classifier degrades gracefully: unrecognized shapes become `Unsupported`, which is always correct. The 54.1.0-verified facts in Context are re-checked on any DataFusion bump.
- [serde/serde_json added to a deliberately minimal staticlib] → Small, ubiquitous, no transitive surprises; if the crate's dependency discipline hardens later, the serializer is swappable for a hand-written writer behind the same recognizer.

## Migration Plan

Purely additive. Existing providers (including `providers/parquet`, `providers/iceberg`, and all examples) implement only `TableProvider` and keep byte-identical behavior; the FFI signature changes are internal to the single link unit and rebuilt together. Rollback = revert the commits. No data or on-disk format concerns.

## Open Questions

- Whether the first pruning consumer (`providers/parquet` row-group skipping and `providers/iceberg` manifest pruning) lands as one follow-up change or two — does not affect this change's specs, approach, or tasks.
- Whether `Decimal128` literals join the supported set in this change's lifetime or the first consumer's — the AST's tagged-literal encoding extends without breaking either side, so this is safely deferrable.
