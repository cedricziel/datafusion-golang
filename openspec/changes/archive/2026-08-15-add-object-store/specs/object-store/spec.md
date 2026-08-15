# object-store Delta

## Purpose

Routes table locations given as URLs (`file://`, `s3://`, `gs://`, `azblob://`, `mem://`) or bare local paths to a pluggable storage backend, giving file-based table providers uniform ranged reads, atomic-feeling writes, and listing over local disk, in-memory storage, S3, GCS, and Azure Blob.

## ADDED Requirements

### Requirement: Resolve a location string to a storage backend

The package SHALL resolve a location string to a storage backend and an in-store object path. A location with no URL scheme, or with the `file` scheme, SHALL resolve to the local-filesystem backend and behave exactly like the same operations performed directly on that path with the `os` package. A location whose scheme is registered SHALL resolve to that scheme's backend. A location whose scheme is not registered SHALL fail with a Go error that names the scheme and the package to import to enable it. A Windows-style drive-letter path (e.g. `C:\data\x.parquet`) SHALL be treated as a local path, not as a URL scheme.

#### Scenario: Bare path resolves to local storage

- **WHEN** a program resolves `/data/orders.parquet`
- **THEN** resolution succeeds with the local backend and reads observe the same bytes as reading the file directly

#### Scenario: file:// URL resolves to local storage

- **WHEN** a program resolves `file:///data/orders.parquet`
- **THEN** resolution succeeds with the local backend addressing `/data/orders.parquet`

#### Scenario: Registered scheme resolves to its backend

- **WHEN** the `mem` scheme is registered and a program resolves `mem://bucket/orders.parquet`
- **THEN** resolution succeeds with the in-memory backend addressing `bucket/orders.parquet`

#### Scenario: Unregistered scheme fails with an actionable error

- **WHEN** a program resolves `s3://bucket/key` in a process that has not enabled the S3 backend
- **THEN** resolution returns a Go error naming the `s3` scheme and the import that enables it

### Requirement: Backends register by URL scheme

The package SHALL provide a registry mapping URL schemes to backend openers. The local (`file` and bare paths) and in-memory (`mem`) backends SHALL be available without additional imports or dependencies. The S3 (`s3`), GCS (`gs`), and Azure Blob (`azblob`) backends SHALL each live in their own subpackage and SHALL register their scheme as an import side effect, so a program enables exactly the cloud dependencies it imports. Programs SHALL also be able to register their own opener for a scheme, and to pass an explicitly constructed backend to a table provider, bypassing the registry.

#### Scenario: Importing a backend subpackage enables its scheme

- **WHEN** a program imports the S3 backend subpackage and resolves `s3://bucket/key`
- **THEN** resolution succeeds with an S3-backed store for `bucket`

#### Scenario: Custom opener

- **WHEN** a program registers its own opener for a scheme and resolves a location with that scheme
- **THEN** resolution returns a store produced by that opener

### Requirement: Reads are ranged, not whole-object

Opening an object for reading SHALL return a handle that supports sequential reads, seeking, random-access reads at absolute offsets, and reports the object's total size. Reading a byte range SHALL NOT require transferring the whole object from the backend. Opening an object that does not exist SHALL fail with a Go error. Opening the handle itself SHALL NOT read the object's data.

#### Scenario: Random-access read

- **WHEN** a program opens an object and reads 8 bytes at an absolute offset near its end
- **THEN** the read returns exactly those bytes, and the backend is not asked for byte ranges outside the requested range beyond the size lookup

#### Scenario: Missing object

- **WHEN** a program opens a path at which no object exists
- **THEN** the open returns a Go error and produces no usable handle

### Requirement: Writes commit on Close and are never partially visible

Creating an object for writing SHALL return a writer whose written bytes become visible at the target path only when Close succeeds. Until then, readers of that path SHALL observe the prior object (or its absence) unchanged. A write that is aborted or whose Close fails SHALL leave the prior state intact and SHALL leave no stray temporary artifacts. On the local and in-memory backends, replacing an existing object SHALL be atomic: a concurrent reader observes the complete old bytes or the complete new bytes, never a mixture. On cloud backends, the same per-object atomicity SHALL hold for readers; coordination between concurrent writers is explicitly not provided — the last successful writer wins.

#### Scenario: No visibility before Close

- **WHEN** a program has written bytes to a new object's writer but not yet closed it
- **THEN** opening the target path fails as missing (or returns the prior object if one existed)

#### Scenario: Successful Close publishes the object

- **WHEN** a program writes an object and Close returns nil
- **THEN** a subsequent open at that path returns exactly the written bytes

#### Scenario: Aborted write leaves prior state

- **WHEN** a program aborts a write over an existing object
- **THEN** the existing object's bytes are unchanged and no temporary artifact remains

### Requirement: Objects can be removed and listed by prefix

The package SHALL support removing an object by path and listing the objects under a path prefix, returning each object's path and size. Listing a prefix with no objects SHALL yield an empty result, not an error.

#### Scenario: Remove then open

- **WHEN** a program removes an existing object and then opens its path
- **THEN** the open fails as missing

#### Scenario: List a prefix

- **WHEN** a program writes objects `a/1` and `a/2` and lists prefix `a/`
- **THEN** the listing yields exactly those two paths with their sizes

### Requirement: In-memory backend for hermetic use

The `mem` scheme SHALL provide a process-local, dependency-free backend suitable for tests: objects written under a `mem://` location SHALL be readable at the same location for the lifetime of the process, with no filesystem or network I/O, and SHALL support the full read, write, remove, and list contract.

#### Scenario: Round-trip through memory

- **WHEN** a program writes an object to `mem://t/x` and reads `mem://t/x` back
- **THEN** the read returns exactly the written bytes without touching disk or network

### Requirement: Cloud credentials resolve via each SDK's standard chain, with URL parameters as overrides

Cloud backends SHALL authenticate using the provider SDK's standard resolution chain (e.g. `AWS_*` environment/shared config for S3, `GOOGLE_APPLICATION_CREDENTIALS`/application-default credentials for GCS, `AZURE_*`/default Azure credential for Azure Blob) with no configuration required beyond the location URL. Connection parameters SHALL be overridable per location through URL query parameters — including at minimum region, endpoint, and path-style addressing for S3-compatible stores — so that S3-compatible services (e.g. MinIO) are reachable without code changes. Locations differing in query parameters SHALL NOT share a resolved backend configuration.

#### Scenario: Custom S3-compatible endpoint

- **WHEN** a program resolves `s3://bucket/key?endpoint=http://localhost:9000&s3ForcePathStyle=true&region=us-east-1` against an S3-compatible server at that endpoint
- **THEN** reads and writes for that location are served by that endpoint using credentials from the standard AWS chain

#### Scenario: No credentials, no ambient config

- **WHEN** a cloud location is resolved in an environment where the SDK's chain yields no credentials
- **THEN** the failure surfaces as a Go error from resolution or the first operation, not a panic or silent empty result
