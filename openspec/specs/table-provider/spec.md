# table-provider Specification

## Purpose
Lets a Go program supply its own data source to DataFusion: expose a schema and rows implemented entirely in Go, queryable from SQL exactly like a built-in table.
## Requirements
### Requirement: Register a Go-implemented table

A session SHALL let a Go program register a named table backed entirely by Go code. Once registered, the table SHALL be queryable by that name in SQL executed on the same session. Registering a second table under a name already in use SHALL return an error rather than silently replacing the existing table.

#### Scenario: Register and query

- **WHEN** a program registers a Go-implemented table named `people` exposing columns `id` and `name`, then executes `SELECT * FROM people`
- **THEN** the query returns exactly the rows the Go implementation produces, with a schema matching `id` and `name`

#### Scenario: Duplicate registration

- **WHEN** a program registers a second table under a name already registered on the same session
- **THEN** registration returns a Go error and the original table remains queryable unchanged

### Requirement: Table schema is exposed before scan

The engine SHALL query the Go table's schema during planning, before requesting any rows, and SHALL use that schema to validate and plan the query (e.g. column references, type checking).

#### Scenario: Query references unknown column

- **WHEN** a program executes `SELECT missing_col FROM people` against a registered table without that column
- **THEN** the query returns a Go error identifying the unknown column, and the Go table's scan is never invoked

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

### Requirement: Errors during scan surface as query errors

An error returned by the Go table implementation while producing rows SHALL abort the query and surface to the caller as a Go error describing the failure. It SHALL NOT crash the process or leave the session unusable for subsequent queries.

#### Scenario: Scan fails partway through

- **WHEN** a Go table's scan returns an error after having already produced some rows
- **THEN** the query fails with a Go error, and the session remains usable for later queries

### Requirement: Registered tables are released with the session

Closing a session SHALL release the engine's references to every table registered on it. A Go table implementation SHALL NOT be invoked after the session that registered it has been closed.

#### Scenario: Close releases table references

- **WHEN** a session with a registered table is closed
- **THEN** the engine holds no further references to the Go table implementation, and any goroutines or resources the Go program owns for that table remain under its own control

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

### Requirement: Opt-in insert support

A Go table provider SHALL be able to declare, at registration time, that it accepts inserts. Whether a provider declares support SHALL be determined solely from the provider given to registration; no separate registration call or configuration SHALL be required. An `INSERT` statement against a registered table that does not declare insert support SHALL fail with a Go error stating the table does not support inserts, without invoking the provider, and the session SHALL remain usable. Declaring insert support SHALL NOT change any read-path behavior.

#### Scenario: Insert into a writable provider

- **WHEN** a program registers an insert-capable provider for table `t` and executes `INSERT INTO t VALUES (...)`
- **THEN** the provider's insert is invoked with the statement's rows and the statement succeeds

#### Scenario: Insert into a non-writable provider

- **WHEN** a program executes `INSERT INTO t VALUES (...)` against a registered provider that does not declare insert support
- **THEN** the statement fails with a Go error indicating the table does not support inserts, the provider is never asked to insert, and the same session still answers `SELECT` queries against the table

### Requirement: Insert input arrives as one stream in the registered schema

For an insert-capable provider, the engine SHALL plan and execute the `INSERT` statement's input itself — both `INSERT INTO t VALUES (...)` and `INSERT INTO t SELECT ...` forms, including selects that read other registered Go tables — and SHALL deliver the resulting rows to the provider as a single Arrow record batch stream, exactly once per statement. The stream's schema SHALL be the table's registered schema: when the statement names a column subset or lists columns out of order, the engine SHALL reorder columns, fill omitted columns with NULL (or the column's default when one is defined), and cast values to the registered column types before the rows reach the provider. An input producing zero rows SHALL be a valid insert of zero rows, not an error.

#### Scenario: Column subset is completed by the engine

- **WHEN** a table with columns `id` and `name` receives `INSERT INTO t (name) VALUES ('x')`
- **THEN** the provider's insert receives one row with the full registered schema — `id` NULL and `name` `'x'` — in the registered column order

#### Scenario: Insert from a query over another registered table

- **WHEN** a program executes `INSERT INTO dst SELECT id, name FROM src` where `src` is another registered Go table
- **THEN** the provider for `dst` receives exactly the rows the query over `src` produces, in `dst`'s registered schema

#### Scenario: Zero-row insert

- **WHEN** an insert's input produces no rows (e.g. `INSERT INTO t SELECT * FROM src WHERE false`)
- **THEN** the statement succeeds, the provider observes an empty input stream, and the reported row count is 0

### Requirement: Insert mode is plumbed through

The engine SHALL pass the statement's insert mode to the provider: append (`INSERT INTO`), overwrite (`INSERT OVERWRITE`), or replace (`REPLACE INTO`). The engine SHALL NOT restrict which modes reach the provider; the provider decides which modes it supports and SHALL be able to reject a mode by returning an error, which fails the statement as a Go error and leaves the session usable.

#### Scenario: Append mode delivered

- **WHEN** a program executes `INSERT INTO t VALUES (...)` against an insert-capable provider
- **THEN** the provider's insert is invoked with the append mode

#### Scenario: Provider rejects an unsupported mode

- **WHEN** a program executes `INSERT OVERWRITE t VALUES (...)` against an insert-capable provider that only supports append and returns an error for other modes
- **THEN** the statement fails with a Go error carrying the provider's message, and the session remains usable for later queries

### Requirement: Insert reports a row count

The provider SHALL report how many rows it wrote, and the statement's SQL result SHALL be a single record batch with one row and one non-nullable UInt64 column named `count` carrying that number, returned through the same result-reader path as any other statement. The statement SHALL NOT return until the provider's insert has completed, so a subsequent query on the same session observes whatever state the provider made visible.

#### Scenario: Count surfaces as the statement result

- **WHEN** an insert-capable provider writes 3 rows for `INSERT INTO t VALUES (...), (...), (...)`
- **THEN** the statement's result is a single row with `count = 3` in a UInt64 column named `count`

#### Scenario: Read-after-write on the same session

- **WHEN** a program inserts rows into a writable provider whose written rows are visible to its own scans, then executes `SELECT` on the same table in the same session
- **THEN** the `SELECT` result includes the inserted rows

### Requirement: Errors during insert surface as statement errors

An error returned by the provider's insert — including a panic in the Go implementation, which SHALL be converted to an error at the boundary — SHALL fail the statement with a Go error describing the failure. It SHALL NOT crash the process or leave the session unusable for subsequent statements. The engine delivers the input stream exactly once per statement and SHALL NOT retry a failed insert; whether rows written before the failure remain visible is the provider's contract to define and document, and the provider MUST perform any commit or rollback it needs before returning.

#### Scenario: Provider insert fails after consuming part of the stream

- **WHEN** an insert-capable provider returns an error after having consumed some of the input rows
- **THEN** the statement fails with a Go error carrying the provider's message, the insert is not retried, and the session remains usable for later queries

#### Scenario: Provider insert panics

- **WHEN** an insert-capable provider's insert panics
- **THEN** the statement fails with a Go error identifying the panic, and the session remains usable for later queries

