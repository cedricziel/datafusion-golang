# table-provider Delta: add-table-provider-pushdown

## MODIFIED Requirements

### Requirement: Full-table scan

Executing a query against a registered table SHALL invoke the Go implementation to produce the table's rows as Arrow record batches, which the engine consumes like any other input. A scan that produces zero rows SHALL be a valid, non-error result.

For a provider that does not declare scan-pushdown support, the scan SHALL remain a full, unprojected, unfiltered read: the engine SHALL apply the query's filters and column projection itself after consuming the provider's batches, and the provider's scan SHALL be invoked with no pushdown information. Existing providers SHALL keep working unchanged without code modification.

#### Scenario: Multi-batch table scan

- **WHEN** a Go table's scan produces more than one record batch
- **THEN** a query selecting all rows from that table returns every row across all batches

#### Scenario: Empty table

- **WHEN** a Go table's scan produces no rows
- **THEN** a query against it returns an empty result with the table's schema, not an error

#### Scenario: Non-pushdown provider with WHERE and column list

- **WHEN** a query with a `WHERE` clause and a column subset runs against a provider that does not declare pushdown support
- **THEN** the provider performs its usual full scan, and the engine filters and projects the result so the query answer is correct

## ADDED Requirements

### Requirement: Opt-in scan pushdown

A Go table provider SHALL be able to declare, at registration time, that it accepts scan pushdown. For a provider that declares support, every scan the engine requests SHALL carry the query's scan options: the projected column set, the pushable filter predicates, and the limit hint, as defined by the requirements below. Whether a provider declares support SHALL be determined solely from the provider given to registration; no separate registration call or configuration SHALL be required.

#### Scenario: Pushdown provider receives scan options

- **WHEN** a program registers a pushdown-capable provider for a table with columns `id` and `name` and executes `SELECT name FROM t WHERE id > 2`
- **THEN** the provider's scan is invoked with a projection identifying the needed columns and a filter predicate equivalent to `id > 2`, and the query returns exactly the rows and columns that a non-pushdown provider with the same data would return

#### Scenario: Scan without filters or limit

- **WHEN** a query against a pushdown-capable provider has no `WHERE` clause and no `LIMIT`
- **THEN** the provider's scan receives an empty filter list and no limit hint, and the projection alone is in effect

### Requirement: Projection pushdown is honored exactly

The engine SHALL pass a pushdown-capable provider the ordered list of column indices (into the registered schema) that the query requires. The provider MUST return record batches containing exactly those columns, in that order, and the engine SHALL NOT re-project the provider's output. If the provider's returned stream schema does not match the expected projected schema, the query SHALL fail with a Go error identifying the mismatch, and the session SHALL remain usable for later queries.

#### Scenario: Projected scan returns only requested columns

- **WHEN** `SELECT name FROM people` runs against a pushdown-capable provider whose table has columns `id` and `name`
- **THEN** the provider's scan receives a projection selecting only `name`, the batches crossing the engine boundary contain only that column, and the query result matches the table's `name` values

#### Scenario: Provider violates the projection contract

- **WHEN** a pushdown-capable provider returns a stream whose schema does not match the requested projection
- **THEN** the query fails with a Go error describing the schema mismatch, and the session remains usable for later queries

#### Scenario: Query needing all columns

- **WHEN** `SELECT *` runs against a pushdown-capable provider
- **THEN** the provider receives a projection covering every column and the query returns the full table

### Requirement: Filter pushdown is advisory

The engine SHALL pass a pushdown-capable provider only filter predicates expressible in the supported predicate forms: comparisons between a column and a literal value (`=`, `!=`, `<`, `<=`, `>`, `>=`), `IS NULL` / `IS NOT NULL`, `[NOT] BETWEEN` with literal bounds, `[NOT] IN` with a literal list, and `AND` / `OR` / `NOT` combinations of these. Predicates outside these forms SHALL be withheld from the provider and evaluated by the engine after the scan, without affecting the delivery of supported predicates from the same query.

Pushed filters SHALL be advisory: the engine SHALL re-apply every pushed filter to the provider's output, so a provider that ignores a filter, applies it partially, or applies it fully produces identical query results. A provider MAY use pushed filters only to omit rows that cannot satisfy them; it MUST NOT be required to evaluate any filter for the query to be correct.

#### Scenario: Provider ignores pushed filters

- **WHEN** a pushdown-capable provider receives filter predicates and returns its full (projected) data unchanged
- **THEN** the query result is identical to the same query against a non-pushdown provider with the same data

#### Scenario: Provider prunes using a pushed filter

- **WHEN** a pushdown-capable provider uses a pushed predicate such as `id > 2` to skip producing batches containing only rows that cannot match
- **THEN** the query returns exactly the rows satisfying the `WHERE` clause, identical to a full scan followed by engine-side filtering

#### Scenario: Unsupported predicate shapes are withheld

- **WHEN** a query's `WHERE` clause combines a supported conjunct (e.g. `id > 2`) with an unsupported one (e.g. `length(name) = 3`) using `AND`
- **THEN** the provider receives only the supported conjunct, the engine evaluates the unsupported one after the scan, and the query result is correct

### Requirement: Limit pushdown is advisory

When the query plan carries a fetch limit down to the table scan, the engine SHALL pass it to a pushdown-capable provider as a hint. The provider MAY stop producing rows once it has produced at least that many, and MAY ignore the hint entirely; the engine SHALL enforce the query's limit on the provider's output regardless.

#### Scenario: Provider truncates at the limit hint

- **WHEN** `SELECT * FROM t LIMIT 3` runs against a pushdown-capable provider that stops after producing 3 rows
- **THEN** the query returns exactly 3 rows

#### Scenario: Provider ignores the limit hint

- **WHEN** `SELECT * FROM t LIMIT 3` runs against a pushdown-capable provider that produces all rows
- **THEN** the query still returns exactly 3 rows
