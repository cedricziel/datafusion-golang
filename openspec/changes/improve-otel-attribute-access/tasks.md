## 1. Map<Utf8, AnyValue> investigation (design D1)

- [ ] 1.1 Prototype `attributes` as `Map<Utf8, anyValueType()>` (arrow-go `array.MapBuilder`) in a throwaway probe; verify construction and a plain `SELECT * FROM wide_events` round-trips correctly (including the array_value/kvlist_value-bearing event)
- [ ] 1.2 Test subscript read access via SQL: `attributes['key']` for a struct-typed map value, and chained field access into it (`attributes['key'].string_value`); test missing-key and NULL-map behavior
- [ ] 1.3 Test the existing `INSERT INTO ... SELECT` ingestion path (provider-to-provider, per the package doc comment's existing rationale) against the map-typed schema — confirm no analogous CAST/concat failure to the ones already hit with `List<Struct>`
- [ ] 1.4 Decide: if 1.1-1.3 all work cleanly, replace `wideEventsSchema`'s attributes column, `appendEvent`'s attribute-writing logic, and `httpEventsView` (or its equivalent) with the map-typed version, removing `UNNEST`/`GROUP BY`/`MAX`/`ARRAY_AGG` entirely; if not, keep `List<Struct>` as-is and write a package-doc-comment note recording exactly what failed and at which step, for a future re-evaluation

## 2. Scalar UDF attribute extraction (design D2)

- [ ] 2.1 Implement a Go `ScalarUDF` extracting a named attribute's value from the physical `attributes` column (signature per design D2 — a single struct-returning UDF if expressible, otherwise per-variant typed UDFs)
- [ ] 2.2 Register and demonstrate it in `examples/otel-wide-events`, with at least one call site exercised against each `AnyValue` variant already covered by the example (string, bool, int, double, bytes; array/kvlist if the UDF's argument marshaling handles nested-in-nested types, otherwise documented as out of scope for the UDF specifically)
- [ ] 2.3 Confirm the UDF's behavior is identical regardless of task group 1's outcome (works the same against `List<Struct>` or `Map`, whichever the example ends up using)

## 3. Finish

- [ ] 3.1 Update the package doc comment to reflect whichever encoding and extraction approach(es) ship, including the D1 negative-result note if applicable
- [ ] 3.2 `make lint && make format`, run `go run ./examples/otel-wide-events`, confirm output is correct
- [ ] 3.3 `/simplify` pass if the diff size warrants it
