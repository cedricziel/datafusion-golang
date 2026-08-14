# Proposal: wire-parquet-and-iceberg-insert

## Why

`add-table-provider-insert` gives the engine an opt-in `WritableTableProvider` channel, but — by that change's explicit Non-Goal — neither real provider uses it: `INSERT INTO` against a registered Parquet file or Iceberg table still fails. This change wires both `providers/parquet` and `providers/iceberg` to accept inserts for the modes their real write APIs can honestly deliver, following the same "engine capability first, then wire the real providers" split that `add-table-provider-pushdown` → `wire-parquet-and-iceberg-pushdown` established.

## What Changes

- `providers/parquet`: the provider implements `datafusion.WritableTableProvider`. Verified against arrow-go v18.7.0: Parquet's footer-at-end format has no append-to-a-closed-file API (`pqarrow.NewFileWriter` only writes a fresh file to an `io.Writer`), so **Append** is implemented as an atomic whole-file rewrite — stream the existing rows, then the insert's rows, into a temp file in the same directory, finalize, and `os.Rename` over the original — and **Overwrite** as the same temp-file-plus-rename with only the new rows. **Replace** is rejected with a clear error (a single Parquet file has no key semantics to replace on). A failure at any point before the rename leaves the original file byte-for-byte untouched.
- `providers/iceberg`: only **catalog-backed** providers (`NewTableProviderFromCatalog`) implement `WritableTableProvider`. Verified against iceberg-go v0.6.0: `Table.Append` and `Table.Overwrite` exist, take an `array.RecordReader`, and commit atomically — but the commit path (`Transaction.Commit` → `Table.doCommit`) calls `CommitTable` on the table's `CatalogIO`, which is `nil` for tables opened via `table.NewFromLocation`, and a pinned `metadata.json` path could never observe the new snapshot anyway. Metadata-location providers therefore stay non-writable at the type level — `INSERT` against them fails with the engine's standard "does not support inserts" error — and their read behavior is unchanged. Catalog-backed providers support **Append** (new snapshot) and **Overwrite** (copy-on-write replacement of all current data in one commit) and reject **Replace** (no key semantics in this provider).
- Read-after-write works within the same provider instance and session for both writable paths: the Parquet provider opens the file fresh per scan, and the catalog-backed Iceberg provider re-resolves the table through the catalog per scan, so a scan after a successful insert sees the written data with no provider or session rebuild.
- Concurrent inserts against the same provider instance are serialized provider-side; scans never block on inserts and observe either the pre-insert or post-insert state, never a torn mix.
- No engine, FFI, or Rust changes; no new dependencies. Both wirings are pure Go against the already-pinned arrow-go v18.7.0 and iceberg-go v0.6.0 write APIs.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `parquet-table-provider`: adds insert support — append and overwrite modes with atomic all-or-nothing file replacement, replace-mode rejection, read-after-write visibility, schema stability across writes, and insert/scan concurrency behavior.
- `iceberg-table-provider`: adds insert support for catalog-backed providers — append and overwrite modes committed as atomic snapshots through the catalog, replace-mode rejection, read-after-write visibility via per-scan catalog resolution — and pins down that metadata-location providers do not declare insert support.

## Impact

- **Code**: `providers/parquet/` (new write path: `insert.go` or similar, plus tests) and `providers/iceberg/` (new write path on the catalog-backed provider type, plus tests). No changes to `datafusion/`, `rust/`, or `include/`.
- **Dependencies**: none new. `pqarrow.FileWriter` ships in the pinned arrow-go v18.7.0; `Table.Append`/`Table.Overwrite` ship in the pinned iceberg-go v0.6.0 (the providers' own tests already use `Table.Append` via `catalog/hadoop` to build fixtures).
- **Depends on**: `add-table-provider-insert` (the engine-level `WritableTableProvider`/`InsertOp` contract this change implements; not yet built — this change is planned against its settled design) and `add-iceberg-catalog-client` (the catalog-backed constructor the Iceberg write path requires; already implemented, change not yet archived).
- **Downstream**: none breaking. Existing read-only behavior of both providers is untouched; a Parquet file rewritten by an insert is re-encoded with the writer's own properties (see design), which affects encoding details, never data.
