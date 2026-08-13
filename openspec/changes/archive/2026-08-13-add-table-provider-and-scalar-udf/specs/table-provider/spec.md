## Purpose

Lets a Go program supply its own data source to DataFusion: expose a schema and rows implemented entirely in Go, queryable from SQL exactly like a built-in table.

## ADDED Requirements

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

#### Scenario: Multi-batch table scan

- **WHEN** a Go table's scan produces more than one record batch
- **THEN** a query selecting all rows from that table returns every row across all batches

#### Scenario: Empty table

- **WHEN** a Go table's scan produces no rows
- **THEN** a query against it returns an empty result with the table's schema, not an error

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
