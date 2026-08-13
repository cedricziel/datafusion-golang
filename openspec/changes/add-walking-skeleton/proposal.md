# Proposal: add-walking-skeleton

## Why

Go has no embedded, extensible SQL query engine comparable to Apache DataFusion; existing bindings are abandoned or shallow. This project aims to expose DataFusion's full power (schemas, UDFs, planner extensions) to Go, and the first step is a walking skeleton that proves the riskiest part end to end: a Rust staticlib linked via cgo, executing SQL and returning results zero-copy through the Arrow C interfaces.

## What Changes

- New Go module `github.com/cedricziel/datafusion-golang` with a public `datafusion` package.
- New Rust crate (workspace under `rust/`) that wraps DataFusion behind a small C ABI, built as a staticlib and linked via cgo.
- `SessionContext` in Go: create, close, and execute SQL; results are returned as an Arrow `RecordReader` crossing the FFI boundary via the Arrow C Stream interface (zero-copy).
- Errors originating in DataFusion/Rust are surfaced as Go errors with the engine's message preserved.
- A runnable example under `examples/` demonstrating SQL execution end to end.
- Build tooling (Makefile) that compiles the Rust staticlib and wires it into `go build` / `go test`, so the library and examples build from a clean checkout with Rust and Go toolchains installed.

## Capabilities

### New Capabilities

- `sql-execution`: Session lifecycle and SQL execution — creating and closing a session context, executing a SQL statement, consuming results as Arrow record batches in Go, and receiving engine errors as Go errors.

### Modified Capabilities

_None — this is the first change in the project._

## Impact

- **Code**: New repository layout — `datafusion/` (Go package), `rust/` (Cargo workspace with the FFI crate), `examples/` (runnable programs), `Makefile`.
- **Dependencies**: Go — `github.com/apache/arrow-go/v18` (Arrow arrays + `cdata`). Rust — `datafusion`, `datafusion-ffi`, `arrow` (FFI feature), `tokio`.
- **Toolchain**: Development requires both Go and a Rust toolchain; prebuilt static archives for end users are explicitly out of scope for this change (tracked for a later change).
- **Downstream**: Establishes the FFI, memory-ownership, and threading patterns (handles, release callbacks, tokio runtime lifecycle) that all later phases (UDFs, table providers, planner hooks) build on.
