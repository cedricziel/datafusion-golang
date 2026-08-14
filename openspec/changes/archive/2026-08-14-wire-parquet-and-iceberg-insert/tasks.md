# Tasks: wire-parquet-and-iceberg-insert

## 1. Parquet write path

- [x] 1.1 Implement the atomic rewrite core in `providers/parquet` (design D1–D3): temp file in the target's directory, `pqarrow.FileWriter` with `WithStoreSchema()` on the registered schema, stream-old-then-new for Append / new-only for Overwrite, fsync + `os.Rename` commit, temp-file removal on any error, input-row counting (D2 short-circuits for zero-row Append)
- [x] 1.2 Implement `InsertInto` on the Parquet provider: per-instance insert mutex (D7), mode dispatch (Append/Overwrite → rewrite core, Replace → clear unsupported-mode error), reader release on all paths
- [x] 1.3 Unit tests, direct-call without the engine (D8 layer 1): Replace rejection; failing-reader injection at first/middle/last batch with before/after file-hash equality and no leftover `.tmp-*`; zero-row Append leaves the file untouched; zero-row Overwrite produces a valid empty file; rewritten-file schema round-trips (fresh provider derives the identical Arrow schema); insert-then-scan returns exact combined (Append) or replaced (Overwrite) row sets
- [x] 1.4 Concurrency tests: two concurrent Appends both land (serialization); a scan opened before an insert completes reads the full pre-insert contents; a scan opened after sees post-insert contents
- [x] 1.5 Differential pushdown regression test: pushdown-enabled queries on a rewritten file agree row-for-row with unpruned scans (guards D3's statistics claim)

## 2. Iceberg write path

- [x] 2.1 Split writability into the type system (design D4): catalog-backed constructor returns a wrapper type adding `InsertInto`; metadata-location constructor keeps the read-only type; add a compile-time/assertion test that the metadata-location provider does NOT satisfy the writable interface
- [x] 2.2 Implement `InsertInto` on the catalog-backed type (D5–D7): insert mutex, fresh `cat.LoadTable` per insert, schema-substituting reader wrapper that installs the canonical registered schema with field-id metadata stripped, `Table.Append` for Append / `Table.Overwrite` (no filter) for Overwrite, Replace → clear unsupported-mode error, input-row counting with zero-row Append short-circuit
- [x] 2.3 Unit tests, direct-call without the engine (D8 layer 2): Replace rejection; failing-reader injection asserting the current snapshot ID is unchanged and a rescan returns pre-insert contents; stub `catalog.Catalog` with always-failing `CommitTable` asserting the insert errors and no new snapshot appears; schema-wrapper behavior with no / full / partial field-id metadata on the input
- [x] 2.4 End-to-end tests against a hadoop-catalog table: Append then scan (combined rows), Overwrite then scan (replaced rows), zero-row Overwrite empties the table, partitioned-table Append with partition-filtered verification, two concurrent Appends both land
- [x] 2.5 Concurrent scan-vs-insert test: a scan started before a commit yields its snapshot's complete row set

## 3. Engine integration (blocked on add-table-provider-insert landing)

- [x] 3.1 Verify both providers' `InsertInto` signatures against the final merged `datafusion.WritableTableProvider`/`InsertOp` and adjust if the engine change's implementation shifted the interface
- [x] 3.2 End-to-end SQL tests per provider: register, `INSERT INTO ... VALUES`, `INSERT INTO ... SELECT` (including from another registered table), column-subset insert (engine fills NULLs), `INSERT OVERWRITE`, count-result assertions, then same-session `SELECT` verifying exact row sets
- [x] 3.3 End-to-end failure tests: `REPLACE INTO` (or trampoline-level Replace mode if the dialect refuses the syntax) fails cleanly against both providers and the session stays usable; `INSERT` against a metadata-location Iceberg provider fails with the engine's not-writable error

## 4. Documentation and finish

- [x] 4.1 Package docs and README: document per-provider mode support, the Parquet append-is-rewrite cost and `.tmp-*` naming, Iceberg's catalog-backed-only writability with a pointer to `catalog/hadoop` for catalog-less local use, and concurrency/visibility semantics (D7)
- [x] 4.2 Update both providers' examples with an insert-then-read snippet
- [x] 4.3 `make lint && make format`, full test suite, then `/simplify` pass over the new code
