# table-provider Delta: add-table-provider-insert

## ADDED Requirements

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
