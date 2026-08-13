## 1. Parquet skip evaluator (design D2, D3)

- [x] 1.1 Define the provider-internal `rowGroupStats` abstraction (per-column optional typed min/max, optional null count, row count) and the `canSkip(expr, stats) bool` entry point in `providers/parquet`
- [x] 1.2 Write the adversarial unit suite against hand-built stats first (failing): every comparison operator at boundary-below/at-min/contained/at-max/above, missing stats, missing null counts, all-null columns, min==max, Between (incl. inverted-bounds and negated), InList (incl. negated), IsNull/IsNotNull, NaN-float guards, unsigned-extreme values, timestamp unit mismatch, Not-rewrites (double negation, De Morgan, negated leaves), And/Or composition, unknown shapes ⇒ keep
- [x] 1.3 Implement the evaluator to green: satisfaction semantics, Not-rewrite into leaves, sort-order-domain comparisons (unsigned bit-reinterpretation, bytewise utf8/binary, unit-matched timestamps), all-null shortcuts, conservative keep on every uncertain path
- [x] 1.4 Implement the `TypedStatistics` → `rowGroupStats` adapter (type-switch over concrete stats types, honoring `Statistics()` nil, `HasMinMax`, `HasNullCount`) with unit tests over stats objects built via `NewStatisticsFromEncoded`/`SetMinMax` (note: decoded zero null counts are ambiguous in arrow-go and are distrusted — see adapter comment)

## 2. Parquet pushdown scan (design D1, D4)

- [x] 2.1 Implement the Arrow-field-index → Parquet-leaf-indices mapping via `FileReader.Manifest.Fields` with unit tests covering flat and nested schemas and projection-order preservation
- [x] 2.2 Implement `ScanWithOptions`: build leaf indices in projection order, evaluate `canSkip` per row group, call `GetRecordReader(ctx, leafIndices, survivingRowGroups)`; short-circuit to an empty projected-schema reader when every row group is skipped; keep the existing per-scan open/close lifecycle (`closingRecordReader`)
- [x] 2.3 Handle the empty (zero-column) projection: read the cheapest surviving column and wrap with `datafusion.ProjectReader(reader, [])`; test row counts survive
- [x] 2.4 Implement the limit-hint truncating reader wrapper and test it stops after the batch reaching the limit

## 3. Parquet bloom-filter pruning (design D5)

- [x] 3.1 Write unit tests (failing) for the physical-value conversion allowlist: utf8/binary → ByteArray, integer widenings, uint32/uint64 bit-reinterpretation, date32, unit-matched timestamps; floats and bools excluded; unit-mismatch ⇒ no probe
- [x] 3.2 Implement the bloom probe for `eq` and `InList` on surviving row groups: `GetBloomFilterReader().RowGroup(rg).GetColumnBloomFilter(leaf)`, nil ⇒ keep, `TypedBloomFilter[T].Check` false ⇒ skip; wire into the row-group decision after min/max pruning; ensure no bloom I/O happens when the scan carries no eq/IN predicate

## 4. Parquet differential and integration tests (design D8)

- [x] 4.1 Build multi-row-group fixture writers (via `pqarrow` with sorted/partition-like data, stats enabled, bloom filters enabled for allowlisted types; include a stats-free column)
- [x] 4.2 Differential battery: pushdown results equal engine-filtered full-scan results row-for-row across the operator battery (comparisons, BETWEEN, IN, IS NULL, AND/OR/NOT, projections in and out of file order, LIMIT)
- [x] 4.3 Assert real pruning happens (surviving row-group list shrinks for provably-disjoint predicates; bloom-absent equality skips) so pruning cannot silently regress to never-skip
- [x] 4.4 Engine-level scenario tests from the delta spec: narrow SELECT column order, empty projection row counts, missing-stats keep, all-null skip, bloom false-positive keep, filterless LIMIT truncation

## 5. Iceberg expression converter (design D6)

- [x] 5.1 Write converter unit tests first (failing): every AST node → expected `iceberg.BooleanExpression`; lossless literal widenings; Between/negated-Between rewrites; all-or-nothing conjunct dropping on any unconvertible literal; `BindExpr` failure ⇒ conjunct dropped, never an error
- [x] 5.2 Implement `datafusion.Expr` → `iceberg.BooleanExpression` conversion with the lossless-only literal mapping and per-conjunct all-or-nothing semantics
- [x] 5.3 Implement scan-setup validation: bind each converted conjunct with `iceberg.BindExpr(schema, expr, true)`, drop on error, AND the survivors into one row filter

## 6. Iceberg pushdown scan (design D6, D7)

- [x] 6.1 Implement `ScanWithOptions`: projection indices → field names, `tbl.Scan(WithSelectedFields(names...), WithRowFilter(expr), WithLimit(n), WithCaseSensitive(true))` with each option applied only when present
- [x] 6.2 Implement output reordering: compute the selected-set schema order, wrap with `ProjectReader` when it differs from the requested order; handle the empty projection via first-field + `ProjectReader(reader, [])`; test both
- [x] 6.3 Keep `Scan` byte-identical and verify non-pushdown scenarios still pass

## 7. Iceberg differential and integration tests (design D8)

- [x] 7.1 Build partitioned multi-data-file fixtures via iceberg-go's write path (partition column + sorted value column, several files per partition)
- [x] 7.2 Differential battery: pushdown results equal engine-filtered full-scan results row-for-row across the operator battery, including filters on projected-away fields and timestamps/timestamptz columns
- [x] 7.3 Engine-level scenario tests from the delta spec: partition pruning correctness, column-statistics pruning correctness, untranslatable-filter drop (results still correct), projection order, empty projection, filterless LIMIT
- [x] 7.4 Adversarial equivalence checks on conversion-sensitive cases: NOT/De Morgan shapes, NOT BETWEEN, NOT IN with NULLs present, equality on timestamp columns at unit boundaries

## 8. Finalize

- [x] 8.1 Update both packages' doc comments (pushdown behavior, advisory-filter contract wording from the table-provider spec) and the repo README/examples if they mention full-scan-only providers
- [x] 8.2 `make lint && make format`, run the full test suite, `openspec validate "wire-parquet-and-iceberg-pushdown" --strict`
