# Add Object Store Backends

## Why

Every file-based table provider (parquet, csv, json, jsonl) reads and writes through raw OS calls (`os.Open`, `file.OpenParquetFile(path)`, `os.CreateTemp` + `os.Rename`), so tables can only live on the local filesystem. Rust DataFusion makes storage transparent through its `object_store` crate — a table location is just a URL (`s3://…`, `file://…`) routed to a pluggable backend. datafusion-golang needs the same: a small ObjectStore abstraction plus a URL-scheme registry so the same providers work against S3, GCS, Azure Blob, local disk, and in-memory storage without per-provider storage code.

## What Changes

- New `objectstore` package: a minimal storage interface (ranged reads via `io.ReaderAt`, streaming commit-on-Close writes, remove, list) plus a URL-scheme registry that resolves a location string to a backend.
- Built-in backends with no new dependencies: local filesystem (`file://` and bare paths) and in-memory (`mem://`, primarily for tests).
- Cloud backends as opt-in subpackages driven by `gocloud.dev/blob`: `objectstore/s3` (`s3://`), `objectstore/gcs` (`gs://`), `objectstore/azure` (`azblob://`). Importing a subpackage registers its scheme, `database/sql`-driver style.
- **BREAKING** Provider constructors (`parquet.NewTableProvider`, `csv.NewTableProvider`, `csv.NewTableProviderWithInferredSchema`, `json.NewTableProvider`, `jsonl.NewTableProvider`) accept a location URL or bare local path and gain a `context.Context` parameter (remote opens do I/O). Bare local paths keep today's behavior.
- Parquet scan/pushdown switches from `file.OpenParquetFile(path)` to `file.NewParquetReader` over the store's ranged-read handle, so footer/statistics/bloom-filter reads stay selective over the network.
- Parquet INSERT replaces temp-file-then-rename with the store's commit-on-Close write: identical atomic-replace behavior on local disk (still temp+fsync+rename under the hood), single-object PUT semantics on object stores (readers see old or new object, never partial; concurrent external writers are last-writer-wins, documented honestly).
- Iceberg provider keeps iceberg-go's own `io.FileIO` abstraction (no second abstraction wrapped around it) but gains property plumbing so cloud-backed metadata locations and catalogs work: constructors accept FileIO properties, and cloud schemes are enabled by importing `iceberg-go/io/gocloud` — which uses the same `gocloud.dev` drivers as our backends.

## Capabilities

### New Capabilities

- `object-store`: the storage abstraction — URL-scheme registry and resolution, ranged reads, commit-on-Close writes, local/mem built-ins, opt-in S3/GCS/Azure backends, credential resolution via each SDK's standard chain plus URL query parameters.

### Modified Capabilities

- `parquet-table-provider`: locations become URLs or bare paths routed through the object store; construction, scan, pushdown, and insert requirements are restated over any registered backend; insert atomicity semantics defined per backend family.
- `csv-table-provider`: open-by-path requirements (explicit and inferred schema) become open-by-location over any registered backend.
- `json-table-provider`: open-by-path requirement becomes open-by-location over any registered backend.
- `jsonl-table-provider`: open-by-path requirement becomes open-by-location over any registered backend.
- `iceberg-table-provider`: metadata-location and catalog constructors accept storage properties so tables whose metadata/data live on registered cloud schemes are readable and (catalog-backed) writable.

## Impact

- New packages: `objectstore`, `objectstore/s3`, `objectstore/gcs`, `objectstore/azure`.
- Modified: `providers/parquet` (parquet.go, pushdown.go, insert.go, bloom/stats/skip read paths), `providers/csv`, `providers/json`, `providers/jsonl`, `providers/iceberg` (constructor options), examples and docs that call the old constructors.
- Dependencies: `gocloud.dev` plus the AWS/GCP/Azure SDKs enter `go.mod` (most are already in the module graph via iceberg-go, which itself depends on `gocloud.dev` v0.45.0); consumer binaries only link the drivers whose subpackages they import. Core `objectstore`, local, and mem backends add no dependencies.
- No SQL-surface change: tables are registered programmatically (`SessionContext.RegisterTable`); there is no CREATE EXTERNAL TABLE today, so no parser work is in scope.
- `table-provider` and `catalog-provider` engine contracts are untouched.
