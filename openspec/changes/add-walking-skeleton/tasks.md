# Tasks: add-walking-skeleton

## 1. Repository scaffolding

- [x] 1.1 `git init`, add `.gitignore` (Go, Rust/target, macOS), commit OpenSpec artifacts
- [x] 1.2 Create Go module `github.com/cedricziel/datafusion-golang` with empty `datafusion/` package
- [x] 1.3 Create Cargo workspace under `rust/` with `datafusion-c-abi` staticlib crate (deps: datafusion, arrow with ffi feature, tokio)
- [x] 1.4 Add Makefile with `build`, `test`, `lint`, `format` targets that cover both toolchains (cargo fmt/clippy, go vet/gofmt)

## 2. Rust C ABI (design D1, D3–D5)

- [x] 2.1 Write `include/datafusion_go.h` declaring `df_session_new`, `df_session_sql`, `df_session_free`, `df_string_free`
- [x] 2.2 Implement session handle: `df_session_new`/`df_session_free` boxing a DataFusion `SessionContext`, lazy shared tokio runtime (`OnceLock`)
- [x] 2.3 Implement `df_session_sql`: `block_on` the query, export results as `FFI_ArrowArrayStream` into the caller-provided struct, error string out-param on failure
- [x] 2.4 Wrap all entry points in `catch_unwind`; add Rust unit test executing `SELECT 1` through the C ABI functions directly

## 3. Go bindings (design D2, D6, D7)

- [x] 3.1 Write failing Go test: `NewSessionContext` → `SQL("SELECT 1 AS one")` → RecordReader with expected schema and value (test drives cgo wiring)
- [x] 3.2 Add cgo glue (`datafusion/cgo.go`): LDFLAGS to Rust staticlib, header include, error-string-to-`error` helper
- [x] 3.3 Implement `SessionContext` with `Close()` (RWMutex + closed flag per D7) and `SQL(query string) (array.RecordReader, error)` importing the stream via `cdata`
- [x] 3.4 Make test 3.1 pass; add tests for: invalid SQL returns error and session stays usable, use-after-close errors, empty result, early reader release
- [x] 3.5 Add concurrency test: parallel queries on one session under `go test -race`
- [x] 3.6 Add debug-build finalizer warning for unclosed handles

## 4. Example and docs

- [x] 4.1 Add `examples/sql/main.go`: create session, run a query, print batches; verify it runs via `make` from clean checkout
- [x] 4.2 Write README: what this is, toolchain prerequisites, build/test instructions, architecture sketch, roadmap (UDFs, providers, planner hooks)

## 5. CI and wrap-up

- [x] 5.1 GitHub Actions workflow: macOS + Linux, cache cargo/go, run `make lint && make test`
- [x] 5.2 Run `make lint && make format`, ensure clean tree, final commit series (semantic commits: scaffolding, rust abi, go bindings, example, ci)
