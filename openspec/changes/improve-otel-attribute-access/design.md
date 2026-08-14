## Context

See proposal.md — Why. `examples/otel-wide-events` currently encodes
`attributes` as `List<Struct<key: Utf8, value: AnyValue>>` (a physical
repeated-KeyValue shape) and reads it back through `httpEventsView`, a
`CREATE VIEW` that does `UNNEST(attributes) AS u` then one
`MAX(CASE WHEN u.key = '...' THEN u.value.<variant> END)` per target
column, `GROUP BY event_id`. `array_value`/`kvlist_value` specifically use
`array_element(ARRAY_AGG(...) FILTER (WHERE u.key = '...'), 1)` instead of
`MAX`, working around DataFusion 54.1.0's `MAX`/`FIRST_VALUE`
accumulator failing to merge `List`-typed state across batches.

Two unverified hypotheses motivate this change, both about whether
DataFusion has cheaper access paths for the actual data shape (one row
per attribute key per event — never a real one-to-many needing
aggregation):

- **`Map<Utf8, AnyValue>` + subscript**: arrow-go has `array.MapBuilder`;
  DataFusion 54.1.0 has a `map()` constructor function and, per its
  function docs, subscript access via `map[key]`. Neither this project
  nor its examples have used either yet — maturity for a value type as
  complex as `AnyValue`'s struct (5 scalar fields + 2 bounded-nested list
  fields) is unverified, in particular whether map value access supports
  chained field access (`attributes['key'].string_value`) the way struct
  field access already does (verified working in the existing example).
- **Scalar UDF extraction**: `datafusion.RegisterScalarUDF` is an
  existing, tested capability (see the "Scalar UDFs" section of the
  README). A Go function receiving the `attributes` column's Arrow value
  per row and returning one extracted variant is unverified only in the
  sense that no scalar UDF in this codebase has taken a nested
  `List`/`Map`-typed argument before — worth confirming the FFI argument
  marshaling handles it before treating this as settled.

## Goals / Non-Goals

**Goals:**
- Determine whether `Map<Utf8, AnyValue>` + SQL subscript access is
  mature enough in DataFusion 54.1.0 to replace `List<Struct<key,value>>`
  and the `UNNEST`/`GROUP BY`/aggregate pivot entirely for this example's
  single-row-per-key access pattern.
- Demonstrate a Go `ScalarUDF`-based attribute-extraction path that works
  today regardless of the Map investigation's outcome.
- Leave a clear, permanent record of what was tried and why (or why not)
  it was adopted — this is exploratory by nature; a negative result is a
  valid, useful outcome, not a blocker to finishing the change.

**Non-Goals:**
- Partitioned/multi-file storage for `providers/parquet` — separate,
  larger change, explicitly deferred by user decision.
- Changing `AnyValue`'s own physical encoding (the struct-of-nullable-
  columns per variant, bounded to one level of array/kvlist nesting) —
  this change is only about the outer key→value container (list vs map),
  not the value type it contains.
- Fixing the underlying DataFusion `MAX`-over-`List` accumulator bug
  upstream — out of this project's control. If Map access replaces the
  `List<Struct>` encoding, the bug becomes moot for this example (no more
  aggregation over `List`-typed columns at all); if it doesn't, the
  existing `ARRAY_AGG`/`array_element` workaround stays as documented.
- Any core library (`datafusion/`, `rust/`) changes — both investigations
  use only capabilities that already exist (map construction is an
  arrow-go/DataFusion SQL question, not an FFI question; scalar UDFs are
  already wired end-to-end).

## Decisions

**D1 — Investigate Map first; the outcome decides whether the view survives.**
Build the `Map<Utf8, AnyValue>` variant of the physical schema, verify
construction (`array.MapBuilder`), a plain `SELECT` round-trip, subscript
read access including chained field access into the `AnyValue` struct,
and the existing `INSERT INTO ... SELECT` path. If all of these work
cleanly, replace `List<Struct<key,value>>` and `httpEventsView` with the
map-typed column and direct subscript expressions in `printHTTPEvents`
(or an equivalent view built from subscripts, whichever reads more
clearly) — this removes `UNNEST`, `GROUP BY`, and both aggregation
functions from the example entirely. If any step fails or behaves
unclearly (e.g. a chained-access parse error, an unsupported cast on
`INSERT`, or map-key-not-found semantics that don't fit), keep the
current `List<Struct>` encoding and record the specific failure in the
package doc comment — a future reader re-evaluating this on a newer
DataFusion version needs to know exactly what to re-test, not just that
"map didn't work." Alternative — commit to Map before verifying: rejected,
the whole point of this investigation is that Map's maturity for a
struct-typed value this complex is unknown; committing first risks
landing on the same kind of undocumented-limitation surprise the `MAX`-
over-`List` bug was.

**D2 — Add the scalar UDF regardless of D1's outcome.**
`otel_attr(attributes, key) -> AnyValue`-shaped extraction (exact
signature depends on what DataFusion's Go UDF registration can express
for a struct return type — plain per-variant UDFs, e.g.
`otel_attr_string(attributes, key) -> Utf8`, are the fallback if a single
polymorphic return type isn't expressible) is valuable independently:
it's usable directly in `SELECT` without a view at all, and demonstrates
that attribute extraction doesn't require SQL-level unnest/pivot
machinery in the first place — a Go function can walk the Arrow value
directly. This also serves as a second, independent check on whichever
physical encoding D1 lands on, since the UDF operates on the
`RecordBatch` value directly and doesn't care whether the column is
`List<Struct>` or `Map`.

## Risks / Trade-offs

- [Map subscript access turns out to be immature for a struct-typed value] →
  Accepted; D1 is structured so this is a documented negative result, not
  a stalled change. The example keeps working exactly as it does today.
- [Scalar UDF argument marshaling for nested `List`/`Map` types is
  unverified] → If it doesn't work cleanly, the UDF task's scope narrows
  to whatever argument shape does work (e.g. operating on a
  pre-projected/pre-unnested column rather than the raw nested
  `attributes` column) rather than blocking the change; the goal is
  demonstrating the capability exists, not a specific signature.
- [Two demonstrated extraction paths (view/subscript and UDF) add example
  complexity] → Deliberate: this is a teaching example, and showing two
  valid approaches with their trade-offs is more useful than picking one
  silently. If review finds this genuinely clutters the example, dropping
  to one path is a documented, easy trim — not a redesign.

## Migration Plan

Purely additive/example-scoped. No dependency, FFI, or core-package
changes. Rollback is reverting the commit(s); nothing outside
`examples/otel-wide-events/main.go` is touched.
