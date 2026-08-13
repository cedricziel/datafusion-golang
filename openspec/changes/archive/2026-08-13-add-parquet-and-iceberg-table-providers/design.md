## Context

See proposal.md — Why. This change adds no FFI surface: both new packages produce plain Go values satisfying the `datafusion.TableProvider` interface already established in `add-table-provider-and-scalar-udf` (`Schema() *arrow.Schema`, `Scan(ctx context.Context) (array.RecordReader, error)`), registered the same way any other provider is (`SessionContext.RegisterTable`). Everything here is pure Go, verified against the real APIs (not memory) of `github.com/apache/arrow-go/v18` v18.7.0 and `github.com/apache/iceberg-go` v0.6.0.

## Goals / Non-Goals

**Goals:**

- Query a local Parquet file and a local-filesystem-backed Iceberg table via SQL with no hand-written format-reading code.
- Keep both formats as opt-in dependencies — importing the core module must not force in Parquet/Iceberg's transitive dependency graph.
- Correct resource lifecycle under repeated and concurrent scans (no fd leaks), matching the concurrency guarantees the engine already requires of any `TableProvider`.

**Non-Goals:**

- Remote object stores (S3/GCS/Azure) for either format.
- Iceberg catalog services (REST/Glue/Hive/SQL) — only the catalog-less `metadata.json`-location path.
- Iceberg snapshot selection/time-travel — always the current snapshot.
- Filter or projection pushdown into either reader (the engine-level `table-provider` contract already has no pushdown; these providers don't change that).
- Writing either format (read-only providers).

## Decisions

**D1 — Two separate packages (`providers/parquet`, `providers/iceberg`), not additions to the core `datafusion` package.**
`iceberg-go`'s `go.mod` declares a large set of direct requires (AWS/GCP/Azure SDKs, multiple SQL drivers, testcontainers, OpenTelemetry, substrait) even though only `table` + `io` are actually imported for the read path used here. Go only compiles what's imported, but `go.mod`/`go.sum` and IDE tooling still see the full graph for any module that imports the core package. Keeping both formats in separate packages means a consumer who only wants core SQL execution plus their own `TableProvider` implementations never pulls that graph in at all.

**D2 — Parquet: `file.OpenParquetFile` + `pqarrow.NewFileReader`, schema cached at construction.**
```go
rdr, err := file.OpenParquetFile(path, false)
fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
schema, err := fr.Schema()
```
Construction opens the file once to validate it and cache the schema (satisfying the "fail at construction, not at scan" requirement and mirroring the existing "schema fetched once, cached" pattern from the table-provider engine contract), then closes it — `Scan` opens its own fresh reader (D4).

**D3 — Iceberg: `table.NewFromLocation` for the catalog-less path.**
```go
fsysF := icebergio.LoadFSFunc(nil, metaLocation)
tbl, err := table.NewFromLocation(ctx, table.Identifier{...}, metaLocation, fsysF, nil)
```
This is the same primitive `catalog/hadoop`'s `LoadTable` uses internally — it's the correct minimal tool for "just point at a metadata.json," not a workaround. Schema and scanning both come off the resulting `*table.Table`.

**D4 — Each `Scan` call opens independent underlying resources; no shared reader across scans.**
Neither `pqarrow.FileReader` nor iceberg-go's scan iterator is documented as safe for concurrent reuse across independent scans. Rather than adding a lock around a shared reader (which would serialize concurrent scans and violate the spec's concurrent-scan requirement), each `Scan` call opens its own `file.Reader`/`table.Scan()` from the provider's stored path/table handle. This costs one extra file-metadata read per scan (acceptable — the alternative is added locking complexity or reuse risk for a component whose concurrency contract isn't documented).

**D5 — Iceberg scan bridges via `array.ReaderFromIter`, and its lazily-surfaced errors are the reader's `Err()`, checked and propagated, not swallowed.**
```go
arrowSchema, itr, err := scan.ToArrowRecords(ctx)   // itr: iter.Seq2[arrow.RecordBatch, error]
rr := array.ReaderFromIter(arrowSchema, itr)         // array.RecordReader
```
`array.ReaderFromIter` is exactly the v18 export for this shape — no hand-rolled adapter needed. Its errors surface during iteration (a missing/corrupt data file fails when its batch is pulled, not up front), which directly satisfies the "errors during scan surface as reader errors" requirement — the provider's `Scan` must not swallow a non-nil `Err()` after the iterator is exhausted.

**D6 — File-handle cleanup wraps the returned reader's `Release`/exhaustion, not a separate `Close` method on the provider.**
`TableProvider` has no `Close()` — its lifecycle is tied to the session (released via the existing engine contract, see `add-table-provider-and-scalar-udf`). Per-scan resources (the Parquet `file.Reader`, any handles iceberg-go's scan opens) are owned by a thin wrapper around the returned `array.RecordReader` that closes them when the wrapper's `Release()` is called or the underlying reader is exhausted — satisfying the no-leak requirement without changing the `TableProvider` interface.

**D7 — Dependency pinning.**
`go.mod` adds `github.com/apache/iceberg-go v0.6.0` and explicitly pins `github.com/apache/arrow-go/v18` to v18.7.0 (iceberg-go v0.6.0 itself pins v18.6.0; Go's MVS resolves to the higher version automatically, but an explicit pin avoids silent drift as either dependency updates). `go mod tidy` after adding both.

## Risks / Trade-offs

- [Opening a fresh reader per scan (D4) adds overhead for repeated scans of the same file/table vs. a shared/cached reader] → acceptable; correctness and simple concurrency safety first. Revisit only if profiling shows scan-open overhead matters.
- [`iceberg-go` v0.6.0's write/table-creation support is unconfirmed — test fixtures for the Iceberg provider may need to be hand-authored on-disk tables (`metadata.json` + manifest list + manifest + one Parquet data file) rather than generated via the library itself] → resolved during implementation (see Open Questions); does not affect the specs or the chosen read-path approach.
- [Scoping out object stores and catalog services narrows real-world usefulness] → deliberate; each is its own natural follow-up change once the local-filesystem path is solid, consistent with how pushdown was deferred in the table-provider engine change.

## Migration Plan

Additive only — new packages, no changes to existing packages or specs. No rollback concerns beyond reverting the commits.

## Open Questions

- Whether Iceberg test fixtures are hand-authored on-disk tables or generated via any write support `iceberg-go` v0.6.0 turns out to have — resolved during implementation by checking the library; does not change the specs, the chosen read-path decisions (D3, D5), or the task breakdown either way.
