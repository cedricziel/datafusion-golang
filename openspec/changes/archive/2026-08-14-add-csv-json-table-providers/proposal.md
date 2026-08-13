## Why

`providers/parquet` and `providers/iceberg` already prove the pattern: wrap a real Go-native format reader as a `datafusion.TableProvider` and it becomes queryable via SQL with zero FFI changes. CSV and JSON/JSONL are the other formats users routinely have lying around, and `github.com/apache/arrow-go/v18` — already a direct dependency of this repo — ships its own CSV reader (`arrow/csv`) and two distinct public JSON-reading facilities (`arrow/array`'s `NewJSONReader` for newline-delimited JSON, `RecordFromJSON` for a single JSON array document). No new external dependency is needed for any of the three formats.

## What Changes

- New package `providers/csv`: `NewTableProvider(path string, schema *arrow.Schema) (datafusion.TableProvider, error)` opens a local CSV file against a caller-supplied schema, and `NewTableProviderWithInferredSchema(path string) (datafusion.TableProvider, error)` infers the schema from the file's header and first data row using `arrow/csv`'s `NewInferringReader`.
- New package `providers/jsonl`: `NewTableProvider(path string, schema *arrow.Schema) (datafusion.TableProvider, error)` opens a local newline-delimited JSON file and streams it via `array.NewJSONReader`. Schema is always caller-supplied — arrow-go's JSON reading has no inference support.
- New package `providers/json`: `NewTableProvider(path string, schema *arrow.Schema) (datafusion.TableProvider, error)` opens a local JSON file whose top level is an array of objects, and reads it via `array.RecordFromJSON` into a single record batch. Schema is always caller-supplied, matching `providers/jsonl`.
- All three are separate, independently-importable packages (not added to the core `datafusion` package or merged into one another), matching the `providers/parquet` / `providers/iceberg` precedent — one format, one small, unambiguous constructor contract, no mode flags to get wrong.
- None of the three implement `PushdownTableProvider` in this change — plain `TableProvider.Scan` only.
- File handles opened by a scan are released deterministically when the returned `RecordReader` is released or fully consumed (matching the `closingRecordReader` pattern from `providers/parquet`), or, for `providers/json`, closed synchronously inside `Scan` since `RecordFromJSON` reads the whole file before returning.

## Capabilities

### New Capabilities

- `csv-table-provider`: Opening a local CSV file as a queryable `TableProvider` — explicit or inferred schema, full-file scan, error and resource-lifecycle behavior, and the specific risk of first-row-only type inference.
- `jsonl-table-provider`: Opening a local newline-delimited JSON (JSON Lines) file as a queryable `TableProvider` — caller-supplied schema, streaming full-file scan, error and resource-lifecycle behavior.
- `json-table-provider`: Opening a local JSON file whose top level is an array of objects as a queryable `TableProvider` — caller-supplied schema, whole-file scan into a single record batch, error and resource-lifecycle behavior.

### Modified Capabilities

_None — these are new clients of the existing `table-provider` engine contract (`RegisterTable`, schema-before-scan, errors-as-Go-errors) established in `add-table-provider-and-scalar-udf`. That contract does not change._

## Impact

- **Code**: New packages `providers/csv/`, `providers/jsonl/`, `providers/json/` (each with its own tests). No changes to `datafusion/`, `rust/`, or `include/`.
- **Dependencies**: None new. All three packages use `github.com/apache/arrow-go/v18` (`arrow/csv`, `arrow/array`), already a direct dependency via `providers/parquet` and `datafusion`'s cdata bridge, already pinned to v18.7.0 in `go.mod`.
- **Scope boundaries**: local filesystem only, single file per table (no directories of files), read-only, no scan pushdown. These are explicit non-goals for this change, consistent with how `providers/parquet` and `providers/iceberg` scoped their first change before pushdown was added later.
