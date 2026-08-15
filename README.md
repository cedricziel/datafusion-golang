# datafusion-golang

Embeds [Apache DataFusion](https://datafusion.apache.org/) — a fast,
extensible SQL query engine written in Rust — in Go. SQL results cross the
language boundary zero-copy via the [Arrow C Stream
interface](https://arrow.apache.org/docs/format/CStreamInterface.html) and
are consumed in Go as [`arrow-go`](https://github.com/apache/arrow-go)
record batches.

Beyond executing SQL, a session can be extended from Go: register a
Go-implemented table provider and query it via SQL, register a
Go-implemented scalar function and call it from SQL, or register a whole
Go-implemented catalog whose schemas and tables the engine discovers
dynamically — with DataFusion calling back into your Go code during query
execution. See [Extensibility](#extensibility). Planner hooks and
aggregate/window UDFs are not yet supported. See [Roadmap](#roadmap).

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

A session can be extended with Go-implemented tables, scalar functions,
and catalogs; DataFusion calls back into your Go code while executing the
query. See `examples/extend` for a complete program.

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

### Scan pushdown (opt-in)

A provider that also implements `datafusion.PushdownTableProvider` is
registered with scan pushdown enabled — no separate configuration, the
capability is detected at `RegisterTable` time:

```go
type PushdownTableProvider interface {
    TableProvider
    ScanWithOptions(ctx context.Context, opts *ScanOptions) (array.RecordReader, error)
}

type ScanOptions struct {
    Projection []int  // column indices into the registered schema; nil = all
    Filters    []Expr // implicit AND; advisory
    Limit      int64  // advisory fetch hint; -1 = none
}
```

The three options have different contracts:

- **Projection is exact**: the returned reader must yield exactly the
  projected columns, in order — the engine validates the schema and fails
  the query on a mismatch (`datafusion.ProjectReader` wraps any reader to
  comply). Projected-away columns never cross the FFI boundary.
- **Filters are advisory**: the engine re-applies every pushed filter
  after the scan, so a provider may use them to skip data (row groups,
  data files), apply them partially, or ignore them — results are
  identical either way. A provider may omit only rows that cannot satisfy
  the filters. Filters arrive as a bounded predicate AST
  (`datafusion.Expr`): column-vs-literal comparisons (`=`, `!=`, `<`,
  `<=`, `>`, `>=`), `IS [NOT] NULL`, `[NOT] BETWEEN`, `[NOT] IN`, and
  `AND`/`OR`/`NOT` combinations. Anything else (functions, casts,
  arithmetic, `LIKE`, ...) is never pushed down and is evaluated by the
  engine after the scan.
- **Limit is advisory**: a fetch hint the provider may truncate at or
  ignore; the engine enforces the query's limit regardless. Because
  pushed filters are re-applied above the scan, the hint is `-1` whenever
  the query's `WHERE` clause was pushed down.

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

### Catalogs

A session can be extended with a whole catalog instead of one table at a
time: register a Go-implemented catalog whose schemas and tables the
engine discovers dynamically, at query time, so SQL can address
`catalog.schema.table` against content not known when the catalog was
registered.

```go
type CatalogProvider interface {
    SchemaNames(ctx context.Context) ([]string, error)
    Schema(ctx context.Context, name string) (schema SchemaProvider, found bool, err error)
}

type SchemaProvider interface {
    TableNames(ctx context.Context) ([]string, error)
    Table(ctx context.Context, name string) (table TableProvider, found bool, err error)
}

err := ctx.RegisterCatalog("warehouse", catalog)
reader, err := ctx.SQL("SELECT * FROM warehouse.sales.orders")
```

`SchemaProvider.Table` returns the exact same `TableProvider` interface
`RegisterTable` accepts — a catalog-discovered table is queried through
the identical `Scan`/`ScanWithOptions` contract, so a program can hand the
same implementation to either registration path with no special-casing.

Nothing is cached at registration: `SchemaNames`/`Schema` and
`TableNames`/`Table` are called fresh for every query that needs them, so
a table (or schema) that appears after registration becomes queryable
without re-registering the catalog. This also means a slow or blocking
catalog implementation degrades planning latency for whatever query
triggered it, the same "no cancellation yet" expectation `Scan` already
has.

`Schema`/`Table` returning `found = false` is not an error — it becomes
the engine's own standard schema/table-not-found error, the same shape an
unqualified reference to a missing table already produces. A catalog is
read-only from SQL's perspective: there is no way to `CREATE SCHEMA` or
`CREATE TABLE` against a Go-registered catalog.

The engine's default catalog name is not special-cased: registering a Go
catalog under it (the name `datafusion` uses by default) replaces the
default catalog outright, and any tables or functions previously
registered via `RegisterTable`/`RegisterScalarUDF` become unreachable
through SQL as a result. Registering under any other name already in use
returns an error and leaves the existing catalog unchanged.

### Writable tables

A registered table can accept `INSERT INTO`, `INSERT OVERWRITE`, and
`REPLACE INTO` by implementing an optional interface alongside
`TableProvider`, the same opt-in shape scan pushdown uses:

```go
type WritableTableProvider interface {
    TableProvider
    InsertInto(ctx context.Context, op InsertOp, rows array.RecordReader) (uint64, error)
}
```

`InsertInto` receives the statement's input already planned by the
engine — an explicit column list resolved and reordered, omitted columns
filled with their default or `NULL`, every value cast to the registered
column type — so the incoming `rows` always matches the table's schema
exactly, regardless of what the `INSERT` statement wrote. `op` is one of
`InsertAppend` (`INSERT INTO`), `InsertOverwrite` (`INSERT OVERWRITE`), or
`InsertReplace` (`REPLACE INTO`); a provider that doesn't support a given
mode returns an error for it — the engine has no opinion about which
modes a table supports.

`InsertInto` is called at most once per statement and must commit or roll
back before returning: the engine delivers the row stream exactly once
and never retries. The returned count becomes the statement's DML result
(`SELECT count` — a single `UInt64` row), used verbatim and not
cross-checked by the engine, since only the provider knows what "written"
means for its own write semantics (a dedup or replace path may legitimately
report fewer rows than it consumed). Providers implementing only
`TableProvider` (or `PushdownTableProvider`) are unaffected: `INSERT`
against them fails with a clear "does not support INSERT" error, and
reads are untouched.

```go
err := ctx.RegisterTable("events", table) // table also implements WritableTableProvider
reader, err := ctx.SQL("INSERT INTO events VALUES (1, 'signed_up')")
```

Both built-in providers implement this interface — see their sections
under [Built-in table providers](#built-in-table-providers) for exactly
which modes each supports and what each mode costs.

### Contracts and limitations

- **Goroutine safety is on you**: like an `http.Handler`, a registered
  `TableProvider`, `WritableTableProvider`, `ScalarUDF`, or
  `CatalogProvider`/`SchemaProvider` may be invoked from multiple
  engine-internal threads concurrently and must be safe for concurrent
  use.
- **Duplicate names are rejected**: registering a table, function, or
  (non-default) catalog name already in use (including built-in function
  names) returns an error and leaves the existing registration unchanged.
- **Lifetime**: the engine holds references to registered implementations
  until `Close()`, which drains in-flight queries first — after `Close`
  returns, your implementation is never invoked again.
- **Pushdown is opt-in**: providers implementing only `TableProvider` get
  full-table scans, with DataFusion filtering and projecting after the
  scan. Providers implementing `PushdownTableProvider` receive
  projection/filters/limit per scan (see "Scan pushdown"); pushed filters
  are always re-applied by the engine, so correctness never depends on
  what a provider does with them.
- **No cancellation yet**: the `context.Context` passed to `Scan` carries
  no deadline or cancellation; a reader that blocks forever hangs its
  query.
- **Fixed signatures**: scalar UDFs declare exact argument/return types —
  no variadic or generic functions, and volatility is fixed to `Volatile`
  (never constant-folded).

## Object storage backends

`providers/parquet`, `providers/csv`, `providers/json`, and `providers/jsonl`
all read and write through the `objectstore` package, so any of them can be
pointed at a bare local path, a `file://` URL, an in-memory `mem://` location
(handy for tests — no filesystem or network I/O), or a cloud URL, uniformly:

```go
import "github.com/cedricziel/datafusion-golang/objectstore"
_ "github.com/cedricziel/datafusion-golang/objectstore/s3"     // enables s3://
_ "github.com/cedricziel/datafusion-golang/objectstore/gcs"    // enables gs://
_ "github.com/cedricziel/datafusion-golang/objectstore/azure"  // enables azblob://
```

Local and `mem://` need no import — they're built into `objectstore`. Each
cloud scheme is opt-in: import its subpackage for the side effect of
registering the scheme (`database/sql`-driver style), and only that
package's SDK dependencies are linked into your binary.

Cloud credentials resolve through each storage SDK's standard chain — for
S3, `AWS_ACCESS_KEY_ID`/`AWS_PROFILE`/shared config/IMDS, the same as the
AWS CLI; for GCS, Application Default Credentials (environment, workload
identity, or `gcloud auth application-default login`), the same as
`gcloud`. Connection details can be overridden per location via URL query
parameters, e.g. for an S3-compatible endpoint like MinIO:

```
s3://my-bucket/data.parquet?endpoint=http://localhost:9000&use_path_style=true&region=us-east-1
```

or a local GCS-compatible endpoint like fake-gcs-server:

```
gs://my-bucket/data.parquet?endpoint=http://localhost:4443
```

Reads are ranged (`io.ReaderAt`), so Parquet's pushdown pruning skips network
bytes on a cloud backend the same way it skips disk reads locally — pruned
row groups are never fetched. Writes commit atomically per object (a reader
sees the complete prior object or the complete new one, never a mix), but
there is no cross-process write coordination: two external writers racing to
overwrite the same cloud object are last-writer-wins, and the losing write's
rows are silently absent. This is a change from a local-only setup only in
the *cross-process* case — a single provider instance's own concurrent
inserts are still serialized internally either way.

## Built-in table providers

Two ready-made `datafusion.TableProvider` implementations ship as separate,
independently-importable packages — importing the core module never pulls
in their dependencies:

### `providers/parquet`

Queries a Parquet object — a bare local path or a URL whose scheme is
registered with `objectstore` (`file://`, `mem://`, and, once imported,
`s3://`/`gs://`/`azblob://`):

```go
import parquetprovider "github.com/cedricziel/datafusion-golang/providers/parquet"

table, err := parquetprovider.NewTableProvider(context.Background(), "/path/to/file.parquet")
if err != nil {
    log.Fatal(err)
}
err = ctx.RegisterTable("people", table)
```

Construction opens and validates the file up front, caching its Arrow
schema; a bad or missing path fails immediately rather than at query time.
Each scan uses a fresh file handle, so concurrent and repeated scans of
the same provider never interfere with each other or leak file
descriptors.

The provider implements `PushdownTableProvider`: projected-away columns
are never decoded, pushed filters skip whole row groups whose column
statistics (min/max, null counts) or bloom filters prove they contain no
matching row, and a limit hint stops the scan early. Skipping is strictly
one-directional — a row group is only skipped when the metadata *proves*
no row can match; missing or inconclusive statistics always keep it — so
query results are identical with and without pruning.

**Writes:** the provider also implements `WritableTableProvider`, supporting
`INSERT INTO` (append) and `INSERT OVERWRITE`; `REPLACE INTO` is rejected
with a clear error. Parquet's footer-at-end format has no API to append to
a closed file, so every insert — append or overwrite — rewrites the whole
table: a complete new object is written through the `objectstore` backend's
commit-on-Close `Writer` (append streams the existing rows first, then the
new ones; overwrite streams only the new ones). On the local backend this is
still exactly a `<name>.tmp-<random>` temp file in the same directory,
`fsync`, then `os.Rename` — object stores commit via a single PUT instead;
see [Object storage backends](#object-storage-backends) for what each
backend family guarantees. A failure at any point — reading the input,
writing, or finalizing — aborts the write and leaves the original object
byte-identical; only a successful commit publishes the new one. This makes
append an O(table size) operation per insert, inherent to the format, not a
shortcut taken here; workloads with frequent small appends are a better
fit for `providers/iceberg`. Concurrent inserts on the same provider
instance are serialized by an internal mutex so two rewrites can never
race the commit; scans never take this lock — one opened before an insert
completes reads the object's pre-insert contents to completion, one opened
after reads the new object. Rewriting does not preserve the original file's
encoding details
(compression codec, bloom filters) — data is preserved exactly, but a
rewritten file loses whatever bloom filters it had, which affects future
scan pruning *effectiveness*, never correctness.

```go
_, err = ctx.SQL("INSERT INTO people VALUES (3, 'carol')")
reader, err := ctx.SQL("SELECT * FROM people")
```

### `providers/iceberg`

Queries an Apache Iceberg table, opened either directly from its
`metadata.json` (no catalog service) or resolved through a catalog client:

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
Parquet provider, each scan is independent and resource-safe under
concurrent or repeated use.

Alternatively, `NewTableProviderFromCatalog` resolves a table by
namespace-qualified identifier through any
`github.com/apache/iceberg-go/catalog.Catalog` implementation — the caller
constructs and configures the concrete client (REST, Hive, Glue, SQL,
Hadoop, ...) itself; `providers/iceberg` depends only on the small
`catalog` interface package, never a specific implementation, so its own
dependency footprint is unaffected by which catalog a caller chooses:

```go
import (
    "github.com/apache/iceberg-go/catalog/rest"
    icebergprovider "github.com/cedricziel/datafusion-golang/providers/iceberg"
)

cat, err := rest.NewCatalog(ctx, "prod", "https://iceberg.example.com/api", rest.WithOAuthToken(token))
if err != nil {
    log.Fatal(err)
}
table, err := icebergprovider.NewTableProviderFromCatalog(ctx, cat, "db", "orders")
if err != nil {
    log.Fatal(err)
}
err = ctx.RegisterTable("orders", table)
```

REST is the catalog client this path is tested and documented against;
Hive, Glue, SQL, and Hadoop catalog clients work through the same
`catalog.Catalog` interface but are not exercised or documented here.
Unlike the metadata.json path (pinned to one specific metadata file for
the provider's lifetime), a catalog-backed provider re-resolves the table
through the catalog on every scan, so a commit made between two scans
becomes visible without reconstructing the provider.

The provider implements `PushdownTableProvider`: pushed filters are
converted to Iceberg expressions and handed to iceberg-go's scan, which
uses them for manifest-level partition pruning, per-data-file statistics
pruning, in-file row-group/bloom pruning, and exact row filtering;
projection is pushed via selected fields so only the needed columns are
read, and the limit hint is passed through. A filter that cannot be
converted faithfully is dropped whole (never approximated), so pushed
filters can only ever widen the scan — results are identical either way.

**Writes:** only a catalog-backed provider (`NewTableProviderFromCatalog`)
implements `WritableTableProvider`; the metadata.json path
(`NewTableProvider`) stays read-only, expressed in the type system rather
than as a runtime error — committing a snapshot requires a catalog to
record the new metadata location, which a table pinned to one
`metadata.json` file has no way to observe anyway. Callers who want
catalog-less local writes use `NewTableProviderFromCatalog` with
[`catalog/hadoop`](https://pkg.go.dev/github.com/apache/iceberg-go/catalog/hadoop)
— filesystem-backed, no catalog service required, the same fixture this
repo's own tests write against:

```go
import "github.com/apache/iceberg-go/catalog/hadoop"

cat, err := hadoop.NewCatalog("local", "/path/to/warehouse", nil)
table, err := icebergprovider.NewTableProviderFromCatalog(ctx, cat, "db", "orders")
err = ctx.RegisterTable("orders", table)

_, err = ctx.SQL("INSERT INTO orders VALUES (1, 'first')")
reader, err := ctx.SQL("SELECT * FROM orders")
```

`INSERT INTO` and `INSERT OVERWRITE` are supported, backed by iceberg-go's
own `Table.Append` / `Table.Overwrite`; `REPLACE INTO` is rejected.
`INSERT OVERWRITE` deletes all existing data files and adds the new data
in one commit — the whole table's history-of-snapshots keeps the deleted
data reachable until it's expired, unlike Parquet's overwrite which is
gone once the old file is replaced. Each insert loads a fresh table
handle from the catalog (never a cached one) and commits through
iceberg-go's own machinery, which supplies atomicity (the current
snapshot is untouched until the metadata swap succeeds), commit-conflict
retries, and partitioned-write routing. A concurrent scan is
snapshot-isolated by construction — it always reads one specific
snapshot's data files to completion, so it never observes a torn mix of
pre- and post-commit rows, regardless of when an insert commits relative
to the scan. Inserts on the same provider instance are serialized by an
internal mutex — not required for correctness (the catalog arbitrates
commits), but it avoids same-instance inserts burning iceberg-go's own
commit-retry budget racing each other.

**Scope boundaries (both providers):** local filesystem only — no S3,
GCS, or Azure object stores. Iceberg catalog services are no longer
categorically out of scope (see `NewTableProviderFromCatalog` above), but
`providers/parquet` remains file-path-only.

Run the bundled example, which registers both a Parquet- and an
Iceberg-backed table and joins across them in one query:

```sh
go run ./examples/parquet-iceberg
```

`examples/otel-wide-events` demonstrates a separate physical/logical schema
split for OpenTelemetry-shaped data: a Parquet-backed wide-events table
whose `attributes` column physically encodes OTel's `AnyValue` (a
struct-of-nullable-typed-columns per variant, nested in a list of
`{key, value}` pairs — no native Arrow union plays well with Parquet), plus
a `CREATE VIEW` that unnests and pivots specific attributes into flat,
semantic-convention-conformant columns (quoted dotted names like
`"http.request.method"`). It also shows a new event landing in the
physical table via `INSERT INTO ... SELECT` from another registered
provider and immediately appearing through the logical view:

```sh
go run ./examples/otel-wide-events
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
  (`df_session_register_table`, `df_session_register_scalar_udf`,
  `df_session_register_catalog`).
- **Callbacks are fixed Go-exported symbols**, not function-pointer
  vtables: the Rust staticlib references `go_table_schema`,
  `go_table_scan`, `go_table_release`, `go_table_insert`,
  `go_scalar_udf_invoke`, `go_scalar_udf_release`, and the catalog/schema
  equivalents (`go_catalog_schema_names`, `go_catalog_schema_lookup`,
  `go_catalog_release`, `go_schema_table_names`, `go_schema_table_lookup`,
  `go_schema_release`), resolved when the final Go binary is linked. Each
  registration carries an opaque `cgo.Handle` identifying the Go
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

- Aggregate and window UDFs
- Cancellation (`context.Context`) wiring for scans and queries
- Planner/optimizer hooks
- Prebuilt binary distribution (no local Rust toolchain required)
