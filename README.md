# datafusion-golang

Embeds [Apache DataFusion](https://datafusion.apache.org/) — a fast,
extensible SQL query engine written in Rust — in Go. SQL results cross the
language boundary zero-copy via the [Arrow C Stream
interface](https://arrow.apache.org/docs/format/CStreamInterface.html) and
are consumed in Go as [`arrow-go`](https://github.com/apache/arrow-go)
record batches.

Beyond executing SQL, a session can be extended from Go: register a
Go-implemented table provider and query it via SQL, or register a
Go-implemented scalar function and call it from SQL — with DataFusion
calling back into your Go code during query execution. See
[Extensibility](#extensibility). Planner hooks, aggregate/window UDFs, and
catalog providers are not yet supported. See [Roadmap](#roadmap).

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

## Extensibility

A session can be extended with Go-implemented tables and scalar functions;
DataFusion calls back into your Go code while executing the query. See
`examples/extend` for a complete program.

### Table providers

Implement `datafusion.TableProvider` and register it:

```go
type TableProvider interface {
    Schema() *arrow.Schema
    Scan(ctx context.Context) (array.RecordReader, error)
}

err := ctx.RegisterTable("people", provider)
reader, err := ctx.SQL("SELECT * FROM people WHERE id > 1")
```

The schema is fetched once, at registration. `Scan` runs once per query
execution and must return a fresh `array.RecordReader`; the engine pulls
batches from it as the query consumes them (genuine streaming, not a full
collect) and releases the reader when the query completes or fails.
Errors returned mid-scan abort the query and surface as a Go error; the
session remains usable afterwards.

### Scalar UDFs

Build a function with a fixed signature and register it:

```go
double, err := datafusion.NewScalarUDF(
    "double",
    []arrow.DataType{arrow.PrimitiveTypes.Int64}, // argument types
    arrow.PrimitiveTypes.Int64,                   // return type
    func(args []arrow.Array) (arrow.Array, error) {
        // vectorized: one output value per input row
        ...
    },
)
err = ctx.RegisterScalarUDF(double)
reader, err := ctx.SQL("SELECT double(id) FROM people")
```

Evaluation is vectorized: the function is invoked once per batch with all
argument columns, and must return one array of the declared return type
with one value per input row. Calls whose argument types don't match the
declared signature are rejected during planning, before the function
runs. Errors — and recovered panics — inside the function abort the query
and surface as Go errors without crashing the process.

### Contracts and limitations

- **Goroutine safety is on you**: like an `http.Handler`, a registered
  `TableProvider` or `ScalarUDF` may be invoked from multiple
  engine-internal threads concurrently and must be safe for concurrent
  use.
- **Duplicate names are rejected**: registering a table or function name
  already in use (including built-in function names) returns an error and
  leaves the existing registration unchanged.
- **Lifetime**: the engine holds references to registered implementations
  until `Close()`, which drains in-flight queries first — after `Close`
  returns, your implementation is never invoked again.
- **No filter or projection pushdown yet**: every scan is a full-table
  scan; DataFusion filters and projects after the scan.
- **No cancellation yet**: the `context.Context` passed to `Scan` carries
  no deadline or cancellation; a reader that blocks forever hangs its
  query.
- **Fixed signatures**: scalar UDFs declare exact argument/return types —
  no variadic or generic functions, and volatility is fixed to `Volatile`
  (never constant-folded).

## Built-in table providers

Two ready-made `datafusion.TableProvider` implementations ship as separate,
independently-importable packages — importing the core module never pulls
in their dependencies:

### `providers/parquet`

Queries a local Parquet file directly:

```go
import parquetprovider "github.com/cedricziel/datafusion-golang/providers/parquet"

table, err := parquetprovider.NewTableProvider("/path/to/file.parquet")
if err != nil {
    log.Fatal(err)
}
err = ctx.RegisterTable("people", table)
```

Construction opens and validates the file up front, caching its Arrow
schema; a bad or missing path fails immediately rather than at query time.
Each `Scan` reads every row across all row groups via a fresh file handle,
so concurrent and repeated scans of the same provider never interfere with
each other or leak file descriptors.

### `providers/iceberg`

Queries a local-filesystem-backed Apache Iceberg table directly from its
`metadata.json`, with no catalog service:

```go
import icebergprovider "github.com/cedricziel/datafusion-golang/providers/iceberg"

table, err := icebergprovider.NewTableProvider(ctx, "/path/to/table/metadata/v1.metadata.json")
if err != nil {
    log.Fatal(err)
}
err = ctx.RegisterTable("orders", table)
```

Construction reads the table's current schema; scanning reads the current
snapshot's data files (no snapshot selection or time travel). As with the
Parquet provider, each `Scan` is independent and resource-safe under
concurrent or repeated use.

**Scope boundaries (both providers):** local filesystem only — no S3,
GCS, or Azure object stores; no Iceberg catalog services (REST/Glue/Hive);
no filter or projection pushdown (consistent with the `TableProvider`
contract in general). These are deliberate non-goals, not missing pieces —
see `openspec/changes/add-parquet-and-iceberg-table-providers/design.md`.

Run the bundled example, which registers both a Parquet- and an
Iceberg-backed table and joins across them in one query:

```sh
go run ./examples/parquet-iceberg
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

- **Custom thin C ABI**, not `datafusion-ffi`'s `abi_stable` types —
  declared in `include/datafusion_go.h`: session lifecycle and SQL
  execution (`df_session_new`, `df_session_sql`, `df_session_free`,
  `df_string_free`) plus extension registration
  (`df_session_register_table`, `df_session_register_scalar_udf`).
- **Callbacks are fixed Go-exported symbols**, not function-pointer
  vtables: the Rust staticlib references `go_table_schema`,
  `go_table_scan`, `go_table_release`, `go_scalar_udf_invoke`, and
  `go_scalar_udf_release`, resolved when the final Go binary is linked.
  Each registration carries an opaque `cgo.Handle` identifying the Go
  implementation.
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

The FFI, memory-ownership, callback, and threading conventions are
established; later phases build on them:

- Filter and projection pushdown into Go table scans
- Aggregate and window UDFs, catalog providers
- Cancellation (`context.Context`) wiring for scans and queries
- Planner/optimizer hooks
- Prebuilt binary distribution (no local Rust toolchain required)
