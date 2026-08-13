## 1. providers/csv

- [x] 1.1 Create `providers/csv` package with `tableProvider` struct (path, cached `*arrow.Schema`) and package doc comment describing scope (explicit or inferred schema, header required, no pushdown)
- [x] 1.2 Implement `NewTableProvider(path string, schema *arrow.Schema) (datafusion.TableProvider, error)`: open file, read header row via a throwaway `csv.NewReader`, validate column count against `schema`, close, cache schema
- [x] 1.3 Implement `NewTableProviderWithInferredSchema(path string) (datafusion.TableProvider, error)`: open file, read through header + first data row via a throwaway `csv.NewInferringReader`, capture and cache the resulting `Schema()`, fail if there is no data row
- [x] 1.4 Implement `Schema() *arrow.Schema` returning the cached schema
- [x] 1.5 Implement `Scan(ctx) (array.RecordReader, error)`: open a fresh `*os.File` + `csv.NewReader` (explicit-schema case) or `csv.NewInferringReader` (inferred case) per scan, batch size 1024 via `WithChunk`
- [x] 1.6 Implement `closingRecordReader` wrapper (embeds the csv `*Reader`, holds the `*os.File`) that closes the file on `Release()` or on `Next()` returning false, mirroring `providers/parquet`'s wrapper
- [x] 1.7 Unit tests: valid explicit-schema file, valid inferred-schema file, missing file, column-count mismatch, header-only file with inferred schema (construction error), multi-row scan correctness, empty data file with explicit schema (zero rows, no error), later-row type mismatch under inference surfaces via `Err()`, repeated scans (no fd leak, e.g. via `/proc/self/fd` count or a high repeat-count loop), concurrent scans from multiple goroutines returning correct independent results

## 2. providers/jsonl

- [x] 2.1 Create `providers/jsonl` package with `tableProvider` struct (path, cached `*arrow.Schema`) and package doc comment describing scope (schema always required, one JSON object per row, no pushdown)
- [x] 2.2 Implement `NewTableProvider(path string, schema *arrow.Schema) (datafusion.TableProvider, error)`: open file, construct a throwaway `array.NewJSONReader`, call `Next()` once to force-decode the first object against `schema` (propagate `Err()` as a construction error if decoding fails), close, cache schema
- [x] 2.3 Implement `Schema() *arrow.Schema` returning the cached schema
- [x] 2.4 Implement `Scan(ctx) (array.RecordReader, error)`: open a fresh `*os.File` + `array.NewJSONReader(file, schema, array.WithChunk(1024))` per scan
- [x] 2.5 Implement `closingRecordReader` wrapper (embeds `*array.JSONReader`, holds the `*os.File`) that closes the file on `Release()` or on `Next()` returning false
- [x] 2.6 Unit tests: valid file, missing file, first-object schema mismatch (construction error), multi-object scan correctness in file order, empty file (zero rows, no error), later-row decode failure surfaces via `Err()` without corrupting earlier rows, repeated scans (no fd leak), concurrent scans from multiple goroutines

## 3. providers/json

- [x] 3.1 Create `providers/json` package with `tableProvider` struct (path, cached `*arrow.Schema`) and package doc comment describing scope (schema always required, top level must be a JSON array, whole file loads as one record batch, no pushdown)
- [x] 3.2 Implement `NewTableProvider(path string, schema *arrow.Schema) (datafusion.TableProvider, error)`: open file, call `array.RecordFromJSON(mem, schema, file)`, close file, release the decoded record (construction only validates), cache schema; propagate a decode or non-array-top-level error as a construction error
- [x] 3.3 Implement `Schema() *arrow.Schema` returning the cached schema
- [x] 3.4 Implement `Scan(ctx) (array.RecordReader, error)`: open file, call `array.RecordFromJSON`, close file via `defer` before returning, wrap the single resulting `arrow.RecordBatch` with `array.NewRecordReader(schema, []arrow.RecordBatch{rec})`
- [x] 3.5 Unit tests: valid file, missing file, top-level-not-an-array (construction error), content-schema mismatch (construction error), multi-element scan correctness in file order (single record batch), empty array (zero rows, no error), decode failure inside `Scan` returns a Go error not a reader, repeated scans (no fd leak — file closes before `Scan` returns), concurrent scans from multiple goroutines

## 4. Cross-cutting

- [x] 4.1 Run `go vet` and existing lint/format tooling (`make lint && make format` per project convention) across all three new packages
- [x] 4.2 Confirm `go.mod`/`go.sum` are unchanged (no new direct or indirect dependency introduced) via `go mod tidy` producing no diff
- [x] 4.3 Add package-level example test (`Example...` or a short `_test.go` usage snippet) for at least one of the three packages, following whatever example convention `providers/parquet` already established, if any
- [x] 4.4 Cross-check all three packages' godoc comments against the final spec wording (schema-required vs. inference, single-batch vs. streaming, non-goals) so package docs and specs don't drift
