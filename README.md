# datafusion-golang

Embeds [Apache DataFusion](https://datafusion.apache.org/) — a fast,
extensible SQL query engine written in Rust — in Go. SQL results cross the
language boundary zero-copy via the [Arrow C Stream
interface](https://arrow.apache.org/docs/format/CStreamInterface.html) and
are consumed in Go as [`arrow-go`](https://github.com/apache/arrow-go)
record batches.

This is a walking skeleton: the first slice of a larger project. It proves
the riskiest part of the design end to end — a Rust staticlib linked via
`cgo`, executing SQL, and returning results as Arrow record batches — but it
does not yet support registering custom data sources, UDFs, or planner
hooks. See [Roadmap](#roadmap).

## Prerequisites

- Go 1.21+
- A Rust toolchain (stable), via [rustup](https://rustup.rs/)
- macOS or Linux (Windows is not yet supported)

There is no prebuilt binary distribution yet: building this library requires
both toolchains.

## Build and test

```sh
make build   # builds the Rust staticlib, then the Go module
make test    # cargo test, then go test ./... -race
make lint    # cargo fmt --check && cargo clippy, go vet + gofmt check
make format  # cargo fmt, gofmt -w
```

`go build` / `go test` alone will fail unless the Rust staticlib has already
been built at `rust/target/release/libdatafusion_c_abi.a` — `cgo`'s
`LDFLAGS` point directly at that path. Run `make rust-build` (or `make
build`) first if you're not using the top-level `make` targets.

## Usage

```go
ctx, err := datafusion.NewSessionContext()
if err != nil {
    log.Fatal(err)
}
defer ctx.Close()

reader, err := ctx.SQL("SELECT 1 AS one")
if err != nil {
    log.Fatal(err)
}
defer reader.Release()

for reader.Next() {
    batch := reader.RecordBatch()
    fmt.Println(batch)
}
```

Run the bundled example:

```sh
make run-example
```

## Architecture

```
Go caller
  │  ctx.SQL(query)
  ▼
datafusion/ (Go package)
  │  cgo call: df_session_sql(session, sql, &out_stream)
  ▼
rust/datafusion-c-abi (Rust staticlib, C ABI)
  │  DataFusion SessionContext.sql(...).collect()
  │  writes result into out_stream as FFI_ArrowArrayStream
  ▼
Arrow C Stream interface (zero-copy handoff)
  ▲
  │  cdata.ImportCRecordReader
Go caller receives an array.RecordReader
```

Key design points (see `openspec/changes/add-walking-skeleton/design.md` for
the full rationale):

- **Custom thin C ABI**, not `datafusion-ffi`'s `abi_stable` types — four
  functions: `df_session_new`, `df_session_sql`, `df_session_free`,
  `df_string_free` (declared in `include/datafusion_go.h`).
- **Results cross as an Arrow C Stream**, imported into a standard
  `array.RecordReader` via `arrow-go`'s `cdata` package — zero-copy.
- **Opaque handles**: every Rust object crossing the boundary is boxed and
  passed as `*mut c_void`. The Go `SessionContext` wraps its handle with a
  `Close() error` method and a debug-build finalizer as a leak backstop.
- **Errors** are returned as heap-allocated C strings (or NULL on success),
  freed via `df_string_free`. Every Rust entry point is wrapped in
  `catch_unwind` — panics never unwind across the FFI boundary.
- **One shared multi-threaded tokio runtime** per process, created lazily;
  `df_session_sql` blocks on it synchronously.
- **Concurrency**: a `SessionContext` is safe for concurrent SQL execution
  from multiple goroutines (guarded by a `sync.RWMutex` plus an atomic
  closed flag, so `Close` can drain in-flight calls).

## Roadmap

This walking skeleton establishes the FFI, memory-ownership, and threading
conventions that later phases build on:

- Registering custom table providers (Go-implemented data sources)
- User-defined functions (UDFs) callable from SQL
- Planner/optimizer hooks
- Prebuilt binary distribution (no local Rust toolchain required)
