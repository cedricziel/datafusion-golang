# parquet-table-provider Delta

## RENAMED Requirements

- FROM: `### Requirement: Open a local Parquet file as a table provider`
- TO: `### Requirement: Open a Parquet object at a storage location as a table provider`

## MODIFIED Requirements

### Requirement: Open a Parquet object at a storage location as a table provider

The package SHALL let a Go program construct a table provider from the location of a Parquet object, where the location is a bare local path or a URL whose scheme is registered with the object-store capability (`file://`, `mem://`, `s3://`, `gs://`, `azblob://`, ...). A bare local path SHALL behave exactly as a `file://` location. Construction SHALL fail with a Go error if the location's scheme is not registered, the object does not exist, is not readable, or is not a valid Parquet object, rather than deferring the failure to the first scan.

#### Scenario: Valid file

- **WHEN** a program constructs a provider from the bare path of a valid local Parquet file
- **THEN** construction succeeds and the resulting provider is ready to register and query

#### Scenario: Valid object on a registered backend

- **WHEN** a program writes a valid Parquet object to a `mem://` location and constructs a provider from that URL
- **THEN** construction succeeds and querying the registered table returns the object's rows

#### Scenario: Missing or invalid file

- **WHEN** a program constructs a provider from a location at which no object exists, or an object that is not valid Parquet
- **THEN** construction returns a Go error and produces no usable provider

#### Scenario: Unregistered scheme

- **WHEN** a program constructs a provider from a URL whose scheme has no registered backend
- **THEN** construction returns a Go error naming the scheme

### Requirement: A failed insert leaves the original file intact

An insert that fails at any point — the input stream erroring mid-consume, a write error, or any failure before the new object's contents are complete and committed — SHALL leave the original object byte-for-byte unchanged and still readable, SHALL fail the statement with a Go error, and SHALL NOT be retried by the provider. The provider SHALL NOT expose a state in which the object is truncated, half-written, or otherwise unreadable, and SHALL leave no temporary artifact of the failed attempt on the backend (for the local backend, no temporary file in the object's directory).

#### Scenario: Input stream fails mid-insert

- **WHEN** the insert's input stream returns an error after the provider has consumed part of it
- **THEN** the statement fails with a Go error, the original object's bytes are unchanged, and a subsequent scan returns the object's prior contents

#### Scenario: No stray temporary files after failure

- **WHEN** an insert into a local-path table fails before completion
- **THEN** no temporary file from the failed attempt remains in the file's directory

#### Scenario: Failed insert on a non-local backend

- **WHEN** an insert into a `mem://`-backed table fails mid-way
- **THEN** the location still holds the prior object's exact bytes and no partial object is observable anywhere in the store

### Requirement: Inserts are serialized; scans see a consistent file

Concurrent inserts against the same provider instance SHALL be serialized by the provider so that each insert's effect is that of running alone — no insert's rows are lost to another insert's rewrite. A scan running concurrently with an insert SHALL observe either the object's pre-insert contents or its post-insert contents in full, never a torn or partially written state; the insert's new contents SHALL become visible only when its commit succeeds, on every backend. The provider does not coordinate with writers outside the provider instance: on the local backend, external concurrent modification of the file is undefined behavior, unchanged from before; on cloud backends, a concurrent external writer to the same object results in last-writer-wins — the losing write's rows are silently absent — and this limitation SHALL be documented rather than hidden.

#### Scenario: Two concurrent appends both land

- **WHEN** two append-mode inserts run concurrently against the same provider instance
- **THEN** both statements succeed and a subsequent scan returns the prior rows plus both inserts' rows

#### Scenario: Scan concurrent with an insert

- **WHEN** a scan is consuming the table while an insert into the same provider completes
- **THEN** the scan yields a complete, uncorrupted row set corresponding to either the pre-insert or post-insert object contents

#### Scenario: Insert into an object-store-backed table round-trips

- **WHEN** a session inserts rows into a registered `mem://`-backed Parquet table and then queries it
- **THEN** the query returns the prior rows plus the inserted rows

## ADDED Requirements

### Requirement: Scans read selectively from the storage backend

Scans SHALL read the object through the storage backend's ranged-read interface rather than downloading the whole object when pruning applies: a scan whose pushed filters prune row groups SHALL NOT request the byte ranges of the pruned row groups' data pages from the backend. Projection SHALL likewise avoid requesting unselected columns' data pages. This SHALL hold identically for every backend, so pushdown pruning saves network transfer on remote stores.

#### Scenario: Pruned row groups are never fetched

- **WHEN** a provider is constructed over an instrumented storage backend recording requested byte ranges, and a query's filter provably excludes a row group via column statistics
- **THEN** the scan completes correctly and no byte range within the excluded row group's data pages was requested

#### Scenario: Behavior identical across backends

- **WHEN** the same Parquet bytes are queried with the same SQL through a local path and through a `mem://` location
- **THEN** both queries return identical results
