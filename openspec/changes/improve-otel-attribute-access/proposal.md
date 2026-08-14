## Why

`examples/otel-wide-events` (added in `wire-parquet-and-iceberg-insert`'s
follow-on work) demonstrates a physical/logical schema split for
OpenTelemetry wide events: attributes are stored physically as
`List<Struct<key, value: AnyValue>>`, and a `CREATE VIEW` unnests and
pivots specific keys into flat, semconv-conformant columns via
`MAX(CASE WHEN u.key = '...' THEN u.value.<variant> END)`.

Two problems surfaced building and reviewing that example, both purely
about how attributes are *accessed*, not how `AnyValue` itself is encoded:

1. The pivot idiom is one `MAX(CASE WHEN ...)` clause per attribute — it
   works but doesn't scale past a handful of semconv keys, and it's the
   reason the example hit a real DataFusion 54.1.0 bug: `MAX`/`FIRST_VALUE`
   over a `List`-typed column (`array_value`/`kvlist_value`) fails with
   "not possible to concatenate arrays of different data types" once the
   underlying scan spans more than one batch — reproducible independent of
   this example (a plain multi-row-group Parquet scan with a nested `List`
   column is enough). The current workaround, `ARRAY_AGG(...) FILTER
   (WHERE ...)` unwrapped with `array_element(..., 1)`, is correct but
   non-obvious, and collects-then-discards non-matches rather than
   short-circuiting the way `MAX` would.
2. Each event has exactly one row per attribute key, so the whole
   unnest-then-aggregate machinery is solving a harder problem than the
   data actually poses — a direct per-row lookup (a map subscript, or a
   scalar UDF call) would be simpler and cheaper if the engine supports
   one cleanly.

This change evaluates two ways to simplify attribute access — a
`Map<Utf8, AnyValue>` physical encoding with native subscript SQL, and a
Go-side `ScalarUDF` for named-attribute extraction — and adopts whichever
prove out, documenting what doesn't.

## What Changes

- Prototype `attributes` as `Map<Utf8, AnyValue>` instead of
  `List<Struct<key, value>>` in `examples/otel-wide-events`; test
  construction, plain `SELECT` read-back, subscript access
  (`attributes['key']`), and the existing `INSERT INTO ... SELECT`
  ingestion path against it. Adopt it (replacing the `CREATE VIEW` pivot)
  if subscript access works cleanly through all of those; otherwise keep
  the current `List<Struct>` encoding and record why in the package doc
  comment, so the investigation isn't silently lost.
- Add a Go `ScalarUDF` that extracts a named attribute's value from the
  physical `attributes` column, demonstrated in the example as a second,
  engine-capability-only way to read attributes (no core changes — scalar
  UDF registration already exists) — independent of which physical
  encoding task 1 lands on.
- No core library (`datafusion/`, `rust/`) changes. This is scoped to
  `examples/otel-wide-events` only; partitioned/multi-file storage for
  `providers/parquet` is a separate, larger change, deliberately deferred.

## Capabilities

No new or modified capabilities — this changes an example program, not a
library capability with its own spec (`skip_specs: true` in
`.openspec.yaml`).

## Impact

`examples/otel-wide-events/main.go` only. No changes to `datafusion/`,
`rust/`, `providers/parquet`, or `providers/iceberg`.
