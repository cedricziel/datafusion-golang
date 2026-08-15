# Tasks — add-object-store

Phases 1–6 are deliberately independent merge units: each leaves `main` green, so this change is a natural fit for a stack of PRs (git-stacked-prs skill) — one PR per phase, 2→3→4→5 largely parallel on top of 1.

## 1. Phase 1 — objectstore core: interface, registry, local + mem backends

- [ ] 1.1 Create `objectstore` package: `Store`/`Object`/`Writer` interfaces (design D1 sketch), `Register`, and `Resolve` with bare-path/`file://`/drive-letter handling and the actionable unregistered-scheme error (design D2)
- [ ] 1.2 Implement the local backend over `os`: `Open` returning `*os.File`-backed `Object`, commit-on-Close `Create` via temp-in-dir + fsync + rename (lifted from providers/parquet/insert.go), `Remove`, `List`
- [ ] 1.3 Implement the mem backend: process-local map, commit-on-Close swap, remove, list; register `mem` scheme
- [ ] 1.4 Write the shared `objectstoretest` contract suite (open-missing errors, no-read-on-open, ranged reads, commit-on-Close visibility, abort leaves prior state, remove, list) and run it against local and mem

## 2. Phase 2 — providers on the abstraction (parity refactor + mem coverage)

- [ ] 2.1 **BREAKING** Rework `providers/parquet` reads: constructors take `(ctx, location)`, resolve through `objectstore`, and parquet.go/pushdown.go open via `file.NewParquetReader(obj, …)`; add a `WithStore` option
- [ ] 2.2 Rework `providers/parquet/insert.go` onto `store.Create` commit-on-Close, deleting the temp/rename code; keep `insertMu` and all failure-cleanup semantics
- [ ] 2.3 **BREAKING** Rework `providers/csv`, `providers/json`, `providers/jsonl` constructors/scans onto `(ctx, location)` + `objectstore` sequential reads, replacing `os.Open`/`*os.File` in the closing-reader types
- [ ] 2.4 Extend provider test matrices to run against `mem://` locations (construction, scan, pushdown-pruning byte-range assertions via an instrumented store, INSERT round-trip and failure atomicity per the spec deltas)
- [ ] 2.5 Update examples and docs for the new signatures; `make lint && make format`; run `/simplify` over the diff

## 3. Phase 3 — S3 backend

- [ ] 3.1 Implement the shared gocloud.dev/blob adapter (`ReaderAt` over `NewRangeReader`, size via `Attributes`, writer, list) and contract-test it against `memblob`
- [ ] 3.2 Add `objectstore/s3`: `s3` scheme registration via gocloud `s3blob` URL opener, query-param passthrough (region, endpoint, path-style), store caching per scheme+authority+query
- [ ] 3.3 Add env-gated S3 integration test (skips without e.g. `OBJECTSTORE_S3_TEST_URL`; runnable against MinIO) covering parquet scan+pushdown and INSERT through SQL
- [ ] 3.4 Document S3 usage (import line, URL forms, credential chain, last-writer-wins caveat)

## 4. Phase 4 — GCS backend

- [ ] 4.1 Add `objectstore/gcs`: `gs` scheme via `gcsblob`, ADC credential chain, query-param passthrough
- [ ] 4.2 Env-gated GCS integration test (fake-gcs-server locally) and docs

## 5. Phase 5 — Azure Blob backend

- [ ] 5.1 Add `objectstore/azure`: `azblob` scheme via `azureblob`, default-credential chain, query-param passthrough
- [ ] 5.2 Env-gated Azure integration test (Azurite locally) and docs

## 6. Phase 6 — Iceberg cloud wiring

- [ ] 6.1 Add storage-property plumbing to `providers/iceberg` constructors (`WithIOProps` or equivalent) feeding `icebergio.LoadFSFunc(props, …)` and catalog-loaded tables
- [ ] 6.2 Env-gated integration test: iceberg table on an S3-compatible endpoint (metadata-location read; catalog-backed insert if the harness allows) via blank import of `iceberg-go/io/gocloud`
- [ ] 6.3 Document enabling cloud schemes for iceberg (blank import, property names, scheme differences: `abfs`/`wasb` vs `azblob`)

## 7. Wrap-up

- [ ] 7.1 Re-run full test suite, `make lint && make format`, `/simplify`
- [ ] 7.2 Validate the change (`openspec validate add-object-store --strict`) and sync/archive per project workflow
