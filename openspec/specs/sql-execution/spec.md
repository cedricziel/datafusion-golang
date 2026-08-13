# sql-execution Specification

## Purpose
Lets Go programs embed the DataFusion query engine: create a session, execute SQL statements, and consume results as Arrow record batches without copying data across the language boundary.
## Requirements
### Requirement: Session lifecycle

The library SHALL let a Go program create a session context bound to a DataFusion engine instance and release it deterministically. After a session is closed, further use of it SHALL return an error rather than crash.

#### Scenario: Create and close a session

- **WHEN** a program creates a session context and later closes it
- **THEN** creation succeeds without requiring any global initialization, and close releases all engine resources associated with the session

#### Scenario: Use after close

- **WHEN** a program executes SQL on a session that has been closed
- **THEN** the call returns a Go error indicating the session is closed, and the process does not crash

### Requirement: SQL execution returns Arrow record batches

The library SHALL execute a SQL statement on a session and return the result as an Arrow record reader yielding zero or more record batches, with the result schema available before the first batch is read. Result data SHALL cross the engine boundary via the Arrow C Stream interface without row-level copying or re-serialization.

#### Scenario: Literal query

- **WHEN** a program executes `SELECT 1 AS one`
- **THEN** it receives a record reader whose schema has a single column `one`, yielding one batch with one row containing the value 1

#### Scenario: Multi-batch result

- **WHEN** a query produces more rows than fit in a single batch
- **THEN** the record reader yields multiple batches that together contain exactly the query result, followed by end-of-stream

#### Scenario: Empty result

- **WHEN** a query matches no rows
- **THEN** the record reader reports the correct result schema and yields end-of-stream with no batches (or only empty batches)

### Requirement: Engine errors surface as Go errors

Errors raised by the engine (SQL parse errors, planning errors, execution errors) SHALL be returned as Go errors carrying the engine's message. Engine errors SHALL never panic, abort, or unwind across the FFI boundary.

#### Scenario: Invalid SQL

- **WHEN** a program executes a syntactically invalid statement such as `SELEKT 1`
- **THEN** the call returns a Go error whose message contains the engine's parse error description, and the session remains usable for subsequent queries

### Requirement: Result resources are released explicitly

Record readers returned from query execution SHALL support explicit release, freeing engine-side memory for the result stream. Releasing a reader before it is fully consumed SHALL be safe.

#### Scenario: Early release

- **WHEN** a program releases a record reader after consuming only part of the result
- **THEN** engine-side resources for the stream are freed and no memory is leaked or double-freed

### Requirement: Concurrent use of a session

A single session context SHALL be safe for concurrent SQL execution from multiple goroutines.

#### Scenario: Parallel queries on one session

- **WHEN** multiple goroutines execute queries on the same session concurrently
- **THEN** each receives its own correct result and no data race or crash occurs

