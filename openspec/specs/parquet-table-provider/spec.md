# parquet-table-provider Specification

## Purpose
Lets a Go program query a local Parquet file directly from SQL, by wrapping it as a `TableProvider` without hand-writing any Parquet-reading code.
## Requirements
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

### Requirement: Schema reflects the Parquet file

The provider's schema SHALL be the Arrow schema derived from the Parquet file's own schema metadata.

#### Scenario: Schema matches file

- **WHEN** a provider is constructed from a Parquet file with a known set of columns and types
- **THEN** the provider's schema has exactly those columns, with types matching the Arrow-equivalent of each column's Parquet type

### Requirement: Scan reads every row across all row groups

A scan carrying no pushdown information SHALL yield every row in the file, regardless of how many row groups the file contains, as one or more Arrow record batches. A scan carrying pushdown information SHALL yield every row from every row group that is not skipped under the row-group skipping requirement, projected per the projection requirement.

#### Scenario: Multi-row-group file

- **WHEN** a provider backed by a Parquet file with more than one row group is scanned with no pushdown information
- **THEN** the resulting record batches together contain every row from every row group, in file order

#### Scenario: Empty file

- **WHEN** a provider is backed by a valid Parquet file containing zero rows
- **THEN** scanning yields the file's schema with no rows, not an error

### Requirement: No file handle leaks across repeated or concurrent scans

Each scan SHALL manage its own file access such that releasing or fully consuming the returned reader releases any OS resources that scan opened, independent of other concurrent or subsequent scans of the same provider.

#### Scenario: Repeated scans

- **WHEN** the same provider is scanned many times in sequence, each reader fully consumed and released
- **THEN** no file descriptors are leaked

#### Scenario: Concurrent scans

- **WHEN** the same provider is scanned concurrently from multiple goroutines
- **THEN** each scan independently returns the full file contents and no scan interferes with another

### Requirement: Provider declares scan pushdown support

The provider SHALL declare scan pushdown support at registration, so that every scan the engine requests carries the query's projection, pushable filter predicates, and limit hint.

#### Scenario: Query with WHERE and column subset reaches the pushdown scan

- **WHEN** a query with a `WHERE` clause and a column subset runs against a registered Parquet provider
- **THEN** the provider's scan receives the projection and the pushable predicates, and the query result is exactly what the same query would return against a non-pushdown provider with the same data

### Requirement: Projection is honored exactly and prunes column decoding

For a scan carrying a projection, the provider SHALL return record batches containing exactly the projected columns, in the requested order, and SHALL NOT decode the data pages of projected-away columns. A nil projection means all columns; an empty projection means zero-column batches that still carry the correct row counts.

#### Scenario: Narrow SELECT returns only requested columns

- **WHEN** a scan requests a projection selecting a subset of the file's columns
- **THEN** the returned batches contain exactly those columns, in the requested order, with values matching the file

#### Scenario: Projection order differs from file order

- **WHEN** a scan requests a projection whose column order differs from the file's column order
- **THEN** the returned batches carry the columns in the requested order, not the file's order

#### Scenario: Empty projection

- **WHEN** a scan requests an empty (zero-column) projection of a non-empty file
- **THEN** the returned batches have zero columns and their row counts sum to the number of rows the scan produces

### Requirement: Row-group skipping is statistics-based and one-directional

For a scan carrying filter predicates, the provider SHALL use per-row-group column statistics (min/max, null counts) to skip reading a row group only when the statistics prove that no row in that row group can satisfy the predicates. Whenever statistics are absent, unreadable, or inconclusive for a decision, the row group SHALL be read. A row group containing at least one row that satisfies the predicates SHALL never be skipped.

#### Scenario: Provably disjoint row group is skipped

- **WHEN** a scan carries the predicate `id > 100` and a row group's `id` statistics report max = 50
- **THEN** that row group is not read, and the query result is identical to a full scan followed by engine-side filtering

#### Scenario: Missing statistics keep the row group

- **WHEN** a scan carries a predicate on a column for which a row group has no statistics
- **THEN** that row group is read in full

#### Scenario: All-null column against a comparison predicate

- **WHEN** a scan carries the predicate `x = 5` and a row group's statistics report every value of `x` as null
- **THEN** that row group is skipped, because no null row can satisfy the comparison

#### Scenario: Inconclusive statistics keep the row group

- **WHEN** a scan carries the predicate `id = 50` and a row group's `id` statistics report min = 1 and max = 100
- **THEN** that row group is read, even if it happens to contain no row with `id = 50`

#### Scenario: Pruned and unpruned scans agree

- **WHEN** the same query runs against the same file with row-group skipping active and with it disabled
- **THEN** both produce identical query results

### Requirement: Bloom filters prune only definite absences

For equality and IN predicates on columns that carry bloom filters, the provider SHALL additionally skip a row group only when the bloom filter reports the probed value definitely absent. A "maybe present" bloom answer, an absent bloom filter, or a column/value shape the bloom probe does not support SHALL keep the row group.

#### Scenario: Bloom filter proves absence

- **WHEN** a scan carries the predicate `trace_id = 'abc'` and the row group's bloom filter for `trace_id` reports the value definitely absent
- **THEN** that row group is not read, and the query result is unchanged

#### Scenario: Bloom filter false positive keeps the row group

- **WHEN** a row group's bloom filter reports "maybe present" for a probed value that the row group does not actually contain
- **THEN** the row group is read, and the engine's re-applied filter removes the non-matching rows

### Requirement: Limit hint truncates the scan

When a scan carries a limit hint, the provider MAY stop producing rows once it has produced at least that many. The rows produced up to that point SHALL be correct and complete batches.

#### Scenario: Filterless LIMIT stops early

- **WHEN** `SELECT * FROM t LIMIT 3` runs against the provider and the scan receives the limit hint 3
- **THEN** the provider stops producing rows after at least 3, and the query returns exactly 3 rows

### Requirement: Provider declares insert support for append and overwrite

The provider SHALL declare insert support at registration, so that `INSERT` statements against a registered Parquet table reach the provider. The provider SHALL accept the append mode (`INSERT INTO`) and the overwrite mode (`INSERT OVERWRITE`), and SHALL reject the replace mode with an error stating the mode is unsupported for a Parquet file; the rejection SHALL fail only that statement and leave the session and the file's contents unchanged. The provider SHALL report the number of rows the statement's input delivered as the statement's row count.

#### Scenario: Append reaches the provider and reports its count

- **WHEN** a program registers a Parquet provider for table `t` and executes `INSERT INTO t VALUES (...), (...)`
- **THEN** the statement succeeds and its result reports a row count of 2

#### Scenario: Replace mode is rejected

- **WHEN** a `REPLACE INTO`-mode insert reaches the provider
- **THEN** the statement fails with a Go error stating the mode is unsupported, the file's contents are unchanged, and the same session still answers queries against the table

### Requirement: Append preserves existing rows and adds the new ones

After a successful append-mode insert, the file SHALL contain every row it contained before the insert plus every row the insert's input delivered, and a scan of the provider SHALL return exactly that combined row set. An append of zero rows SHALL succeed, report a count of 0, and leave the table's visible contents unchanged.

#### Scenario: Insert then read returns old and new rows

- **WHEN** a table backed by a file containing rows A and B receives an append-mode insert of rows C and D, and the same session then executes `SELECT` on the table
- **THEN** the result contains exactly rows A, B, C, and D

#### Scenario: Zero-row append

- **WHEN** an append-mode insert's input produces no rows
- **THEN** the statement succeeds with a count of 0 and a subsequent scan returns the file's prior contents unchanged

### Requirement: Overwrite replaces the file's contents with the input rows

After a successful overwrite-mode insert, a scan of the provider SHALL return exactly the rows the insert's input delivered — none of the file's prior rows. An overwrite of zero rows SHALL succeed and leave the table empty (schema intact, zero rows).

#### Scenario: Overwrite then read returns only new rows

- **WHEN** a table backed by a file containing rows A and B receives an overwrite-mode insert of row C, and the same session then executes `SELECT` on the table
- **THEN** the result contains exactly row C

#### Scenario: Zero-row overwrite empties the table

- **WHEN** an overwrite-mode insert's input produces no rows
- **THEN** the statement succeeds with a count of 0 and a subsequent scan returns the table's schema with zero rows

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

### Requirement: Schema is stable across writes

The provider's registered schema SHALL NOT change as a result of an insert, and a scan performed after a successful insert SHALL produce record batches in exactly the registered schema — same fields, same types, same order — so that the engine's schema validation of post-insert scans succeeds. Constructing a new provider from the rewritten file SHALL derive the same Arrow schema as the original file.

#### Scenario: Post-insert scan schema matches the registered schema

- **WHEN** a session inserts rows into a registered Parquet table and then queries it
- **THEN** the query succeeds and its result schema is identical to the schema of the same query before the insert

#### Scenario: Rewritten file round-trips its schema

- **WHEN** a new provider is constructed from a file that a previous provider's insert rewrote
- **THEN** the new provider's schema equals the schema the original provider was constructed with

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

### Requirement: Scans read selectively from the storage backend

Scans SHALL read the object through the storage backend's ranged-read interface rather than downloading the whole object when pruning applies: a scan whose pushed filters prune row groups SHALL NOT request the byte ranges of the pruned row groups' data pages from the backend. Projection SHALL likewise avoid requesting unselected columns' data pages. This SHALL hold identically for every backend, so pushdown pruning saves network transfer on remote stores.

#### Scenario: Pruned row groups are never fetched

- **WHEN** a provider is constructed over an instrumented storage backend recording requested byte ranges, and a query's filter provably excludes a row group via column statistics
- **THEN** the scan completes correctly and no byte range within the excluded row group's data pages was requested

#### Scenario: Behavior identical across backends

- **WHEN** the same Parquet bytes are queried with the same SQL through a local path and through a `mem://` location
- **THEN** both queries return identical results

