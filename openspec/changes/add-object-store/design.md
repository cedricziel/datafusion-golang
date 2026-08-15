# Design — add-object-store

## Context

See proposal.md — Why. Salient facts about the current tree that shape the design:

- `providers/parquet` opens files with `file.OpenParquetFile(path, false)` in three places (parquet.go:48, parquet.go:73, pushdown.go:26). arrow-go's `file.NewParquetReader(r parquet.ReaderAtSeeker, …)` accepts any `io.ReaderAt + io.Seeker`, so the reader is already storage-agnostic one call below the surface. The pushdown machinery (pushdown.go, stats.go, bloom.go, skip.go) only consumes the `file.Reader`'s metadata — it needs selective ranged reads to stay useful over a network.
- `providers/parquet/insert.go` commits via `os.CreateTemp` in the target directory + `fsync` + `os.Rename` (insert.go:84–147). Rename-based atomicity does not exist on object stores.
- `providers/csv`, `providers/json`, `providers/jsonl` use `os.Open` and read strictly sequentially; none needs random access.
- `providers/iceberg` already routes all I/O through iceberg-go's `io.FileIO` scheme registry (`icebergio.LoadFSFunc(nil, metadataLocation)`, iceberg.go:128) — but passes `nil` properties and nothing imports a cloud IO implementation, so only `file://`/bare paths work today.
- iceberg-go v0.6.0 ships `io/gocloud`, which registers `s3`/`s3a`/`s3n`/`oss`, `gs`, and `abfs`/`abfss`/`wasb`/`wasbs` schemes on blank import, implemented over `gocloud.dev/blob` (gocloud.dev v0.45.0 is already in iceberg-go's go.mod). Its `File` interface already requires `io.ReaderAt`.
- Tables are registered programmatically (`SessionContext.RegisterTable`); there is no `CREATE EXTERNAL TABLE` SQL surface, so "table LOCATION" means "the string passed to a provider constructor".
- Project constraints: Go-native only (no cgo/Rust), no backwards-compat shims, pre-1.0 breaking changes are fine.

## Goals / Non-Goals

**Goals:**

- One storage abstraction shared by parquet/csv/json/jsonl; iceberg composes with it rather than being forced under it.
- Ranged reads good enough that parquet pushdown pruning still pays off remotely (footer + selected row groups, not whole-file downloads).
- Honest, per-backend-family write semantics for INSERT.
- Zero new dependencies for the default (local + mem) path; cloud SDKs only for those who import the backend.

**Non-Goals:**

- No `CREATE EXTERNAL TABLE`/SQL LOCATION parsing (no such SQL surface exists yet).
- No multi-file/directory-partitioned tables, globbing, or listing-driven scans — the `List` operation is specified minimally so the abstraction doesn't preclude them, but partitioned-table support is a separate change.
- No caching layer, retry policy tuning, or request coalescing (each SDK's defaults apply).
- No cross-writer coordination on object stores (no locking service, no conditional-put protocol).
- No HDFS, WebDAV, or other exotic backends.

## Decisions

### D1: Library choice — thin own interface; gocloud.dev/blob drives the cloud backends; local and mem are hand-written

Three candidates were evaluated:

1. **`gocloud.dev/blob` (Go CDK) as the abstraction.** Ships `s3blob`, `gcsblob`, `azureblob`, `fileblob`, `memblob` with a URL-opener registry — the closest Go analog to Rust's `object_store`. But `blob.Bucket` is a large API surface, its `Reader` is not an `io.ReaderAt` (parquet's hard requirement — an adapter is needed regardless), and `fileblob` has local quirks (escaping, attrs sidecar files) we don't want in the default path.
2. **Hand-rolling over aws-sdk-go-v2 / cloud.google.com/go/storage / azure-sdk-for-go.** Maximum control, maximum maintenance: three credential chains, three multipart-upload implementations, three list paginations — all code gocloud already maintains, for no observable behavioral difference at our altitude.
3. **iceberg-go's `io.FileIO` as the base for everything.** Already a dependency, already has a scheme registry and `ReaderAt`-capable `File`. But it is iceberg-go's internal contract (v0.6.x, moving), its registry is a global that panics on duplicate registration, its config is an untyped iceberg-flavored `props map[string]string`, and coupling csv/json to an iceberg library inverts the layering.

**Chosen:** a small `objectstore` interface owned by this repo — the stable seam providers compile against — with three backend families behind it:

- **local** (`file://`, bare paths): hand-written over `os` (~100 lines). `Open` returns the `*os.File` (already `ReaderAt+Seeker`); `Create` implements commit-on-Close as temp-in-same-dir + fsync + rename, i.e. exactly insert.go's current commit moved behind the interface. Zero dependencies, zero behavior change for existing users.
- **mem** (`mem://`): hand-written map of path → bytes with commit-on-Close swap. Deterministic, dependency-free test backend.
- **s3 / gcs / azure** (`s3://`, `gs://`, `azblob://`): one shared `gocloud.dev/blob` adapter (`ReaderAt` via `bucket.NewRangeReader`, size via `bucket.Attributes`, writes via `bucket.NewWriter`, list via `bucket.List`), plus three thin registration subpackages that import only their driver. The adapter is generic, so it is testable against `memblob` without cloud credentials, and the per-cloud packages contain almost no logic.

Rationale over (1)-pure: we need the adapter layer anyway for `ReaderAt`; owning the interface keeps the core dependency-free and lets local stay `os`-native. Over (2): gocloud's drivers give streaming multipart uploads, listing, and each SDK's standard credential chain for free. Over (3): iceberg keeps its own FileIO (see D6) — the two abstractions meet at the same underlying gocloud drivers instead of wrapping each other.

Interface sketch (final shape may vary in names only):

```go
package objectstore

type Store interface {
    Open(ctx context.Context, path string) (Object, error)   // read
    Create(ctx context.Context, path string) (Writer, error) // write; visible only after successful Close
    Remove(ctx context.Context, path string) error
    List(ctx context.Context, prefix string) iter.Seq2[ObjectInfo, error]
}

type Object interface { // satisfies parquet.ReaderAtSeeker
    io.ReadSeekCloser
    io.ReaderAt
    Size() int64
}

type Writer interface {
    io.Writer
    Close() error  // commit; no partial object is ever observable
    Abort() error  // discard without committing
}
```

### D2: Locations are URLs resolved through a scheme registry; bare paths mean local

`objectstore.Resolve(location string) (Store, string, error)` splits a location into a store and an in-store path:

- No scheme, or `file://` → local backend. Existing call sites like `parquet.NewTableProvider(ctx, "/data/x.parquet")` keep working (modulo the new `ctx`, see below).
- `s3://bucket/key`, `gs://bucket/key`, `azblob://container/key`, `mem://bucket/key` → registry lookup by scheme; the opener receives the full URL (authority + query) and returns a store bound to that bucket/container. Resolved stores are cached per scheme+authority(+query).
- Unregistered scheme → error naming the scheme and the subpackage to import (mirroring iceberg-go's registration hint), e.g. `objectstore: scheme "s3" not registered (import github.com/cedricziel/datafusion-golang/objectstore/s3)`.

Registration is `objectstore.Register(scheme string, opener Opener)`; the s3/gcs/azure subpackages call it from `init()`, so a blank import enables a scheme — the `database/sql` driver pattern, and the same pattern iceberg-go uses. Windows-style paths (`C:\…`) must not be misparsed as schemes (single-letter scheme → treat as local path).

**BREAKING:** provider constructors gain a leading `context.Context` (construction validates the object, which is now network I/O) — `parquet.NewTableProvider(ctx, location)`, `csv.NewTableProvider(ctx, location, schema)`, `csv.NewTableProviderWithInferredSchema(ctx, location)`, `json.NewTableProvider(ctx, location, schema)`, `jsonl.NewTableProvider(ctx, location, schema)`. Pre-1.0, no compat shims. `providers/iceberg.NewTableProvider` already takes a ctx.

Alternative considered: a per-`SessionContext` store registry (Rust DataFusion's `RuntimeEnv::register_object_store`). Rejected for now: providers are constructed before/independently of sessions here, and a process-global registry with an explicit `WithStore(store)` provider option as the escape hatch (for tests and custom-configured buckets) is simpler and sufficient.

### D3: Credentials — each SDK's standard chain, plus URL query parameters; no config framework

- Default: whatever the SDK resolves — AWS: `AWS_ACCESS_KEY_ID`/`AWS_PROFILE`/shared config/IMDS; GCP: `GOOGLE_APPLICATION_CREDENTIALS`/ADC; Azure: `AZURE_STORAGE_ACCOUNT` + `DefaultAzureCredential`. This is what gocloud's URL openers already do; we add nothing.
- Per-location overrides ride on the URL query string, passed through to the gocloud opener: `s3://bucket/key?region=eu-central-1&endpoint=http://localhost:9000&s3ForcePathStyle=true` (the MinIO/localstack case), `azblob://container/key?storage_account=…`. Query params are part of the store cache key.
- Programmatic escape hatch: construct a store directly (e.g. `s3.NewStore(bucket, awsCfg)` or wrap any `*blob.Bucket`) and either `Register` it under a scheme+authority or hand it to a provider via `WithStore`.

No YAML/struct config system, no credential storage of our own.

### D4: Read path — `Object` is a `parquet.ReaderAtSeeker`; sequential formats just `Read`

- Parquet: all three `file.OpenParquetFile` call sites become `store.Open` + `file.NewParquetReader(obj, …)`. Each `ReadAt` on a cloud object issues one ranged GET (`blob.NewRangeReader(ctx, key, off, n)`), so a pushdown-pruned scan costs: one `Attributes` (size) + footer read + one range per surviving row-group column chunk — pruning skips network bytes, which is the entire point. arrow-go's reader already buffers/coalesces column-chunk reads; we do not add our own caching (Non-Goal).
- CSV/JSON/JSONL: strictly sequential; they use `Object` as a plain `io.ReadCloser` (one streaming GET). The existing `closingRecordReader` pattern carries over with `Object` in place of `*os.File`.
- `Object` carries the resolution-time `ctx` for subsequent `ReadAt`s (gocloud range reads need one); scan APIs already thread `ctx`.

### D5: Write path — commit-on-Close everywhere; atomicity is per-backend-family and documented

The interface contract is: **nothing is observable at the target path until `Close` returns nil; a failed/aborted write leaves the prior object (or absence) intact.** How each family delivers that:

- **local**: temp file in the target's directory + `fsync` + `os.Rename` — byte-for-byte today's insert.go behavior, now inside the backend. Atomic replace, crash-safe.
- **mem**: buffer, then swap the map entry under a mutex on Close. Atomic replace.
- **s3/gcs/azure**: `blob.NewWriter` streams a (multipart) upload that materializes the object only on successful `Close` — cloud object PUTs are per-object atomic, so readers see the old object or the new one, never a torn mix. What is **not** provided (and is documented on the spec level, not papered over): read-modify-write isolation across concurrent writers. Parquet Append = read old + write new + PUT; two concurrent appenders from different processes are last-writer-wins and one append is silently lost. The existing `insertMu` still serializes writers within one provider instance; cross-process coordination is explicitly out of scope (conditional-put/etag protocols would be a future change).

Alternatives considered for cloud Append: write-then-verify (read back etag) — detects but cannot prevent the race, adds a round trip, still last-writer-wins on retry; rejected. Multipart-copy server-side append — S3-only, not portable; rejected.

### D6: Iceberg — keep iceberg-go's FileIO; meet at the drivers, don't wrap

iceberg-go's table scan/commit machinery accepts only its own `io.FileIO`; forcing our `Store` under it would mean adapting our interface back into theirs — two abstractions and an adapter for zero gain. Instead:

- `providers/iceberg` constructors gain option plumbing for FileIO properties (`WithIOProps(map[string]string)` or equivalent), passed to `icebergio.LoadFSFunc(props, location)` (today hardcoded `nil`) and available for catalog-loaded tables via catalog properties.
- Cloud schemes are enabled the way iceberg-go designed: blank-importing `github.com/apache/iceberg-go/io/gocloud`. We document this next to our own backend imports; we do not re-export or auto-import it (keeps iceberg's cloud deps opt-in too).
- Because iceberg-go's cloud IO and our backends are both gocloud.dev-based, one set of SDK dependencies and one credential-resolution behavior serves both.
- Scheme note: iceberg-go registers `abfs`/`wasb` (Hadoop-style) for Azure while our registry uses `azblob://`; iceberg locations use iceberg's schemes (they live inside iceberg metadata anyway), ours use ours. No translation layer.

### D7: Testing strategy

- Provider behavior across backends: run the existing provider test matrices against `mem://` locations in addition to local paths — this is what the mem backend exists for, and it exercises the full URL-resolution path with no network.
- The shared gocloud adapter: contract-tested against `memblob` (a real gocloud driver, in-memory), covering ranged reads, commit-on-Close, abort, and list — i.e. the code that s3/gcs/azure share — without credentials.
- Per-cloud drivers (URL opening + credential wiring, ~no logic): env-gated integration tests (skip unless e.g. `OBJECTSTORE_S3_TEST_URL` is set), runnable against MinIO/fake-gcs-server/Azurite locally.
- A shared store contract-test suite (`objectstoretest`) asserts identical semantics (open-missing errors, commit-on-Close visibility, abort leaves prior state) across local, mem, and the gocloud adapter.

## Risks / Trade-offs

- [Naive `ReadAt` usage could turn one file read into hundreds of GETs] → parquet is the only random-access consumer and arrow-go reads column chunks in large contiguous ranges; the contract test asserts `Open` itself performs no data reads. If profiling later shows chatter, a buffered-footer optimization can be added inside the backend without touching providers.
- [Cloud Append is last-writer-wins across processes] → not fixable without a coordination protocol; stated in the spec as an explicit limitation instead of implied safety. Local keeps full rename atomicity, so existing users lose nothing.
- [go.mod grows cloud SDKs even for local-only users] → module-graph only; binaries link only imported drivers, and most of these modules are already in the graph via iceberg-go. Core stays dependency-free.
- [gocloud.dev abstraction gaps (e.g. no seekable reader, driver quirks)] → we consume a deliberately narrow slice (range reader, writer, attributes, list) behind our own interface; if a driver ever disqualifies itself, the backend can be reimplemented on the raw SDK without changing the `Store` seam.
- [Breaking constructor signatures churn every call site, example, and doc] → pre-1.0 policy allows it; the churn is mechanical (`ctx` threading) and confined to one change.
- [iceberg-go FileIO API instability (v0.6.x)] → the coupling already exists today; this change only passes properties through it, adding no new surface area to break.

## Open Questions

- Whether `List` should be part of `Store` v1 or split into an optional `ListableStore` interface — decidable during implementation; the spec only requires listing to exist for the backends that support it (all of them do), not where the method lives.
- Exact option-name spelling (`WithStore`, `WithIOProps`) — godoc-level detail.
