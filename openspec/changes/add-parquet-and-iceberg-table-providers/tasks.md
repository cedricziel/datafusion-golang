# Tasks: add-parquet-and-iceberg-table-providers

## 1. Dependency setup

- [x] 1.1 Add `github.com/apache/iceberg-go@v0.6.0`, pin `github.com/apache/arrow-go/v18` to v18.7.0 explicitly in `go.mod`, run `go mod tidy`, confirm `go list -deps ./...` doesn't pull cgo-only packages (e.g. `mattn/go-sqlite3`) for the code paths actually used (design D1, D7 gotcha)

## 2. Parquet table provider

- [x] 2.1 Write failing test: `providers/parquet.NewTableProvider` on a valid temp Parquet file (written via `arrow-go`'s own Parquet writer in the test, no committed fixture) succeeds and exposes the expected schema
- [x] 2.2 Implement `NewTableProvider(path string) (datafusion.TableProvider, error)`: open + validate via `file.OpenParquetFile` + `pqarrow.NewFileReader`, cache schema, close the validation reader (design D2)
- [x] 2.3 Implement `Scan`: open a fresh `file.Reader`/`pqarrow.FileReader` per call, return `GetRecordReader(ctx, nil, nil)` wrapped so `Release()`/exhaustion closes the underlying file (design D4, D6)
- [x] 2.4 Add tests: missing file error, invalid/corrupt file error, multi-row-group file returns all rows, empty file returns schema with zero rows
- [x] 2.5 Add resource-lifecycle tests: repeated sequential scans leak no file descriptors (e.g. assert via `/proc/self/fd` count on Linux or a bounded-iteration soak on macOS), concurrent scans under `go test -race` each return correct full results
- [x] 2.6 Register a Parquet-backed provider on a real `SessionContext` and query it via SQL end to end (integration test using the existing `datafusion` package)

## 3. Iceberg table provider

- [x] 3.1 Determine test-fixture strategy: check whether `iceberg-go` v0.6.0 has table-creation/write support usable to generate a local test table; if not, hand-author a minimal valid on-disk table (`metadata.json` + manifest list + manifest + one Parquet data file) under `providers/iceberg/testdata/` (resolves design.md's Open Question)
- [x] 3.2 Write failing test: `providers/iceberg.NewTableProvider(ctx, metadataLocation)` on a valid local test table succeeds and exposes the expected schema
- [x] 3.3 Implement `NewTableProvider(ctx context.Context, metadataLocation string) (datafusion.TableProvider, error)` via `table.NewFromLocation` (design D3), caching the Arrow schema
- [x] 3.4 Implement `Scan`: fresh `tbl.Scan().ToArrowRecords(ctx)` per call, bridged via `array.ReaderFromIter`, wrapped so resources are released on `Release()`/exhaustion (design D4, D5, D6)
- [x] 3.5 Add tests: missing/invalid metadata location error, current-snapshot scan spanning multiple data files returns all rows, empty snapshot returns schema with zero rows
- [x] 3.6 Add error-propagation test: snapshot referencing a missing/corrupt data file surfaces as a reader error, not a silently truncated result (design D5)
- [x] 3.7 Add resource-lifecycle and concurrency tests mirroring 2.5
- [x] 3.8 Register an Iceberg-backed provider on a real `SessionContext` and query it via SQL end to end

## 4. Example, docs, and wrap-up

- [x] 4.1 Add an example (extend `examples/extend` or add `examples/parquet-iceberg`) demonstrating both providers registered and queried via SQL
- [x] 4.2 Update README: document `providers/parquet` and `providers/iceberg`, their scope boundaries (local filesystem only, current snapshot only, no pushdown), and that they're separate importable packages
- [x] 4.3 Run `make lint && make format`, ensure `go test ./... -race` passes across all packages (including the new ones) and pre-existing tests aren't regressed, ensure clean tree
- [ ] 4.4 Commit series (semantic commits: dependency setup, Parquet provider, Iceberg provider, example/docs)
