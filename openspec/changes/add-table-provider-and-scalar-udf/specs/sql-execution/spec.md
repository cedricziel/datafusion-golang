## MODIFIED Requirements

### Requirement: Session lifecycle

The library SHALL let a Go program create a session context bound to a DataFusion engine instance and release it deterministically. After a session is closed, further use of it SHALL return an error rather than crash. Closing a session SHALL also release the engine's references to every table and scalar function registered on it.

#### Scenario: Create and close a session

- **WHEN** a program creates a session context and later closes it
- **THEN** creation succeeds without requiring any global initialization, and close releases all engine resources associated with the session, including any registered tables and scalar functions

#### Scenario: Use after close

- **WHEN** a program executes SQL on a session that has been closed
- **THEN** the call returns a Go error indicating the session is closed, and the process does not crash

### Requirement: Concurrent use of a session

A single session context SHALL be safe for concurrent SQL execution from multiple goroutines. This SHALL hold even when the queries executed invoke registered Go table providers or scalar functions, which the engine may call from multiple engine-internal threads concurrently.

#### Scenario: Parallel queries on one session

- **WHEN** multiple goroutines execute queries on the same session concurrently
- **THEN** each receives its own correct result and no data race or crash occurs

#### Scenario: Parallel queries invoking a registered extension

- **WHEN** multiple goroutines concurrently execute queries that call the same registered Go table provider or scalar function
- **THEN** each query receives its own correct result and no data race or crash occurs, even though the engine may invoke the Go implementation from concurrent engine threads
