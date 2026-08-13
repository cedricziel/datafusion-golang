## Context

See proposal.md — Why. All three new packages produce plain Go values satisfying `datafusion.TableProvider` (`Schema() *arrow.Schema`, `Scan(ctx context.Context) (array.RecordReader, error)`), registered via the existing `SessionContext.RegisterTable`, exactly like `providers/parquet` and `providers/iceberg`. No FFI surface changes.

This design is grounded in the actual `github.com/apache/arrow-go/v18` v18.7.0 APIs, checked via `go doc` and source reading against the module cache rather than assumed from memory:

**CSV** (`arrow-go/v18/arrow/csv`, fully public):
```go
func NewReader(r io.Reader, schema *arrow.Schema, opts ...Option) *Reader
func NewInferringReader(r io.Reader, opts ...Option) *Reader
```
Both return a `*Reader` that already satisfies the `array.RecordReader` shape (`Schema`, `Next`, `RecordBatch`, `Err`, `Retain`, `Release`) — no adapter needed, same as `pqarrow.FileReader` in the Parquet provider. `WithHeader`, `WithComma`, `WithChunk`, `WithColumnTypes`, `WithNullReader` and others exist as options.

Critically, for `NewInferringReader`, reading the source (`reader.go`) shows type inference (`validate()` → `conversionColumn.inferType`) runs exactly once, gated on `r.bld == nil`, using only the **first data row's** string value per column, walking a fixed type ladder (int64 → bool → date32 → time32 → timestamp variants → float64 → string → binary) until a candidate parses. Every subsequent row is parsed against that fixed type; the reader's own doc comment confirms a value that fails to parse under the inferred type produces a null field and halts iteration on the following `Next()` call, with the failure exposed only via `Err()`. This is a real, load-bearing behavior, not an edge case invented for this document — see D2/D3.

**JSONL** (`arrow-go/v18/arrow/array`, fully public, despite living in `arrow/array` rather than a `csv`-like top-level package):
```go
func NewJSONReader(r io.Reader, schema *arrow.Schema, opts ...Option) *JSONReader
```
Doc: "expects to find one json object per row of dataset." Implementation wraps `encoding/json`'s streaming `Decoder` (via arrow-go's `internal/json` type-aliasing shim) and calls `RecordBuilder.UnmarshalOne` per consecutive JSON value in the stream — this is exactly JSON Lines semantics (whitespace/newline-separated JSON values), and `*JSONReader` itself satisfies `array.RecordReader` directly. `WithChunk` batches N objects per record batch (chunk=1 default, i.e. one row per batch unless configured). **No inference variant exists** — `schema` is a required, non-optional parameter.

**JSON (array-of-objects document)** (`arrow-go/v18/arrow/array`, fully public):
```go
func RecordFromJSON(mem memory.Allocator, schema *arrow.Schema, r io.Reader, opts ...FromJSONOption) (arrow.RecordBatch, int64, error)
```
Default mode (no `WithMultipleDocs`) requires the JSON stream's top level to be exactly one `[...]` array of objects; it decodes the *entire* document synchronously and returns a single `arrow.RecordBatch` plus a byte offset — not a `RecordReader`, and there is no batching/chunking knob for this mode. Also schema-required, no inference.

**`internal/json`**: genuinely a Go `internal/` package (path contains `/internal/`) inside the arrow-go module — the Go toolchain forbids importing it from outside that module tree, confirmed by its location, not merely assumed. It is not usable directly and is not needed: `array.NewJSONReader` and `array.RecordFromJSON` already wrap it behind public API and are the actual, sole surface this design uses.

## Goals / Non-Goals

**Goals:**

- Query a local CSV file, a local JSON Lines file, and a local JSON (array-of-objects) file via SQL with no hand-written parsing code, using only `arrow-go` (already a direct dependency).
- Be honest in the spec about arrow-go's real inference behavior for each format rather than presenting a false symmetry between CSV (which infers) and JSON/JSONL (which do not).
- Correct resource lifecycle under repeated and concurrent scans, matching `providers/parquet`/`providers/iceberg`.

**Non-Goals:**

- Scan pushdown (`PushdownTableProvider`) for any of the three. CSV/JSON have no on-disk statistics or indexes to prune with; pushdown here would only mean "decode everything, then project/filter after," which the engine's own post-scan projection and filtering already does for free on a plain `TableProvider`. Explicitly deferred, not silently omitted.
- Remote object stores (S3/GCS/Azure) — local filesystem only, matching `providers/parquet`/`providers/iceberg`.
- Writing any of the three formats — read-only, matching every provider built so far.
- Multiple files or a directory of files as one logical table. `providers/parquet`/`providers/iceberg` scoped to single-file/single-table first; CSV/JSONL's common "directory of daily files" use case is real but adds its own requirements (file ordering, schema reconciliation across files, partial-failure handling) that deserve their own change once single-file is solid.
- Headerless CSV and non-comma delimiters. Real CSV dialect variance exists, but `providers/parquet`/`providers/iceberg` shipped with zero caller-facing read options and earned pushdown as a later, separate change; the same incremental discipline applies here. `csv.Option` pass-through (delimiter, header presence, null-value lists) is easy to add later without breaking the constructor signature, since it would be additional variadic options, not a signature change.
- Multi-row (or whole-file) sampling for CSV type inference. Arrow-go's public `NewInferringReader` doesn't offer it; reimplementing multi-row inference by hand (buffering rows, widening a type lattice, then constructing a second pass reader) is a meaningfully different and heavier feature than "call the library's inferring reader," and belongs in a follow-up if first-row-only inference proves too weak in practice.

## Decisions

**D1 — Three separate packages (`providers/csv`, `providers/jsonl`, `providers/json`), not one package with a mode flag.**
Unlike Parquet vs. Iceberg (D1 of the prior change), all three formats here already live in the same core `arrow-go` dependency, so the "keep consumers from pulling in unneeded dependency footprint" motivation for `providers/parquet`/`providers/iceberg`'s split doesn't directly apply. The split is kept anyway, for a different reason: JSON and JSONL are read through genuinely different arrow-go entry points with different performance and memory characteristics (`array.NewJSONReader` streams in chunks; `array.RecordFromJSON` materializes the whole file as one record batch) and different structural requirements (newline-separated values vs. one top-level array). A single package with a `Mode` enum would let a caller pass a JSONL file with `ModeJSON` (or vice versa) and get a confusing parse error instead of a clear "wrong package for this file shape" mismatch at the type level. Three small, single-purpose constructors — one per format — keep each contract unambiguous, matching the precedent's "one format, one clear contract" philosophy even though the underlying dependency-footprint rationale doesn't carry over unchanged.

**D2 — CSV: explicit-schema `NewTableProvider` is the safe default; `NewTableProviderWithInferredSchema` is offered but documents the first-row-only risk.**
```go
func NewTableProvider(path string, schema *arrow.Schema) (datafusion.TableProvider, error)
func NewTableProviderWithInferredSchema(path string) (datafusion.TableProvider, error)
```
Construction for the explicit-schema path opens the file, reads only the header row (via a throwaway `csv.NewReader`), and validates its column count against the schema — cheap, matches the "validate structure, fail fast" spirit of `providers/parquet`'s D2 without needing to touch data rows. Construction for the inferred-schema path necessarily reads through the header **and** the first data row (arrow-go's `validate()` doesn't populate `r.schema` until then, per Context above), so `Schema()` cannot be available before that point no matter how construction is implemented; the provider does this once at construction (not lazily at first `Scan`) so callers get a stable, cached schema and an early failure if the file is empty of data rows — same "fail before scan" guarantee `providers/parquet` gives, just unavoidably one row deeper for this format because arrow-go's CSV schema is not self-describing metadata like Parquet's footer.

**D3 — CSV: the first-row-only inference limitation is treated as a documented behavior, not silently hidden.**
Rather than either (a) claiming stronger inference than arrow-go provides, or (b) hand-rolling a multi-row sampling inference pass, this design surfaces the real behavior directly in the spec (`csv-table-provider` Requirement "Values incompatible with the inferred type surface as an error, not silent bad data") and requires the provider to let `Err()` propagate rather than swallow it. A caller who needs robust inference across heterogeneous data has the explicit-schema constructor as the reliable path.

**D4 — JSONL: thin wrapper around `array.NewJSONReader`, batched at 1024 rows like Parquet's `BatchSize`.**
```go
func NewTableProvider(path string, schema *arrow.Schema) (datafusion.TableProvider, error)
```
Construction opens the file, constructs a throwaway `array.NewJSONReader`, and calls `Next()` once to force-decode the first object against the schema (catching a schema/content mismatch at construction, matching the "fail before scan" requirement), then discards that reader — `Scan` opens an independent fresh reader per D6 below, consistent with `providers/parquet`'s D4 (no shared reader across concurrent scans). `WithChunk(1024)` is used internally, matching the Parquet provider's hardcoded `BatchSize: 1024`; not exposed as a caller option, for the same "don't add config nobody asked for yet" reason CSV read-dialect options are deferred (see Non-Goals).

**D5 — JSON (array document): `array.RecordFromJSON` runs entirely inside `Scan`; no `closingRecordReader` wrapper needed.**
```go
func NewTableProvider(path string, schema *arrow.Schema) (datafusion.TableProvider, error)
```
Because `RecordFromJSON` is synchronous and returns a single, already-fully-decoded `arrow.RecordBatch` (not a lazy reader), `Scan` can open the file, call `RecordFromJSON`, close the file immediately via `defer`, and only then wrap the resulting single batch with `array.NewRecordReader(schema, []arrow.RecordBatch{rec})` — a pure in-memory `RecordReader` that owns no file handle at all. This is a genuine simplification versus CSV/JSONL (D6): there is no window during which a returned reader still holds an open file descriptor, so the no-leak requirement is met trivially rather than via a wrapper. Construction similarly opens, decodes, and closes the file once to validate content against the schema and cache it — this means construction for this provider does the same work `Scan` will do, an intentional and acceptable duplication (see Risks) mirroring `providers/parquet`'s D2 pattern of opening once at construction and again at each scan.

**D6 — CSV/JSONL: file-handle cleanup wraps the returned reader's `Release`/exhaustion, matching `providers/parquet`'s `closingRecordReader` exactly.**
Both `csv.Reader` and `array.JSONReader` are opened fresh per `Scan` call (no shared reader across concurrent scans, mirroring `providers/parquet`'s D4) and wrapped in a small type embedding the reader plus the `*os.File`, closing the file when `Release()` is called or the stream is exhausted via `Next()` returning false — the identical pattern `providers/parquet` already established, reused rather than reinvented.

**D7 — No new dependency, no `go.mod` change.**
All three packages import only `github.com/apache/arrow-go/v18`'s `arrow/csv` and `arrow/array` subpackages, both already reachable from the existing v18.7.0 pin. `go mod tidy` should be a no-op for direct requires (these subpackages were already indirectly present).

## Risks / Trade-offs

- [CSV inference samples only the first data row; a column that looks like an integer in row 1 but holds a float or string later produces nulls from that row onward and an early-terminated scan (D3)] → acceptable and documented in the spec as a first-class scenario, not a hidden gotcha; the explicit-schema constructor is the answer for heterogeneous data, and multi-row inference is an explicit Non-Goal for this change.
- [JSON array-of-objects mode has no chunking — a large file becomes one giant in-memory record batch (D5)] → acceptable for this change's file-size expectations (same "local file, no object-store streaming" scope as Parquet/Iceberg); if large single-JSON-array files turn out to matter, JSONL is the streaming alternative and should be recommended in package docs.
- [Construction re-reads work that `Scan` will redo — the header/first-row for CSV, the first object for JSONL, the whole file for JSON (D2/D4/D5)] → same trade-off `providers/parquet`'s D4 already accepted (fresh reader per scan costs a metadata re-read); here it costs a bit more for JSON specifically since construction fully decodes the file once. Acceptable — correctness and construction-time fail-fast take priority over avoiding a second file read, consistent with prior precedent.
- [Three near-identical packages instead of one shared internal helper] → conscious duplication over a premature shared abstraction; the `closingRecordReader` pattern is copied, not extracted into a shared internal package, because `providers/parquet` and `providers/iceberg` didn't share one either and the three readers' underlying types differ enough (`*csv.Reader`, `*array.JSONReader`, no reader at all for the JSON-array case) that a shared abstraction would be thinner than the duplication it removes.

## Migration Plan

Additive only — new packages, no changes to existing packages or specs. No rollback concerns beyond reverting the commits.
