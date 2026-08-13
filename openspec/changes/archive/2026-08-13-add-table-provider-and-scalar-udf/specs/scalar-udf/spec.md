## Purpose

Lets a Go program supply its own scalar function to DataFusion: given fixed input types and an output type, transform values entirely in Go, callable from SQL like a built-in function.

## ADDED Requirements

### Requirement: Register a Go-implemented scalar function

A session SHALL let a Go program register a named scalar function with a fixed input argument type signature and output type. Once registered, the function SHALL be callable by that name in SQL executed on the same session. Registering a second function under a name already in use SHALL return an error rather than silently replacing the existing function.

#### Scenario: Register and call

- **WHEN** a program registers a Go-implemented scalar function `double` taking one integer argument and returning an integer, then executes `SELECT double(21)`
- **THEN** the query returns `42`

#### Scenario: Duplicate registration

- **WHEN** a program registers a second function under a name already registered on the same session
- **THEN** registration returns a Go error and the original function remains callable unchanged

### Requirement: Evaluation is vectorized over batches

The engine SHALL invoke the Go function once per batch of input values, passing all values for that batch together, rather than once per row. The function SHALL return one output value for every input value it receives, in the same order.

#### Scenario: Function applied across a batch

- **WHEN** a query calls a registered scalar function on a column with multiple rows in one batch
- **THEN** the Go function is invoked with the batch's values and returns a matching number of output values, each computed from the corresponding input

#### Scenario: Function applied across multiple batches

- **WHEN** a query's input to a registered scalar function spans more than one batch
- **THEN** the function is invoked separately per batch and the query result contains one correctly computed output per input row across all batches

### Requirement: Argument and return type checking

A call to a registered scalar function with argument types that do not match its declared signature SHALL be rejected during query planning, before the function is invoked.

#### Scenario: Wrong argument type

- **WHEN** a program calls a registered scalar function that declares an integer argument, passing a string literal instead
- **THEN** the query returns a Go error during planning, and the function is never invoked

### Requirement: Errors during evaluation surface as query errors

An error returned by the Go function while evaluating a batch SHALL abort the query and surface to the caller as a Go error describing the failure. It SHALL NOT crash the process or leave the session unusable for subsequent queries. A panic inside the Go function SHALL be treated the same way: recovered and surfaced as a query error, never propagated as a crash.

#### Scenario: Function returns an error

- **WHEN** a registered scalar function returns an error while evaluating a batch
- **THEN** the query fails with a Go error, and the session remains usable for later queries

#### Scenario: Function panics

- **WHEN** a registered scalar function panics while evaluating a batch
- **THEN** the query fails with a Go error instead of crashing the process, and the session remains usable for later queries

### Requirement: Registered functions are released with the session

Closing a session SHALL release the engine's references to every scalar function registered on it. A Go function implementation SHALL NOT be invoked after the session that registered it has been closed.

#### Scenario: Close releases function references

- **WHEN** a session with a registered scalar function is closed
- **THEN** the engine holds no further references to the Go function implementation
