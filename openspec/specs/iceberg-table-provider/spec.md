# iceberg-table-provider Specification

## Purpose
Lets a Go program query a local-filesystem-backed Apache Iceberg table directly from SQL, by wrapping it as a `TableProvider` without a catalog service or hand-written Iceberg metadata handling.
## Requirements
### Requirement: Open a local Iceberg table from its metadata location

The package SHALL let a Go program construct a table provider from the local filesystem path to an Iceberg table's `metadata.json`, without requiring a catalog service. Construction SHALL fail with a Go error if the metadata cannot be read or parsed as valid Iceberg table metadata.

#### Scenario: Valid table metadata

- **WHEN** a program constructs a provider from the metadata location of a valid local Iceberg table
- **THEN** construction succeeds and the resulting provider is ready to register and query

#### Scenario: Missing or invalid metadata

- **WHEN** a program constructs a provider from a path that does not exist, or a file that is not valid Iceberg table metadata
- **THEN** construction returns a Go error and produces no usable provider

### Requirement: Schema reflects the table's current schema

The provider's schema SHALL be the Arrow-equivalent of the Iceberg table's current schema, as recorded in its metadata.

#### Scenario: Schema matches table metadata

- **WHEN** a provider is constructed from a table with a known current schema
- **THEN** the provider's schema has exactly those fields, with types matching the Arrow-equivalent of each Iceberg field's type

### Requirement: Scan reads the table's current snapshot across all its data files

A scan carrying no pushdown information SHALL yield every row visible in the table's current snapshot, across all of that snapshot's data files, as one or more Arrow record batches. A scan carrying pushdown information SHALL yield every current-snapshot row that can satisfy the pushed filters, projected per the projection requirement. This change does not support selecting a different snapshot.

#### Scenario: Snapshot spanning multiple data files

- **WHEN** a provider's current snapshot is backed by more than one data file and is scanned with no pushdown information
- **THEN** the resulting record batches together contain every row from every data file in that snapshot

#### Scenario: Empty snapshot

- **WHEN** a provider's current snapshot contains no data files or no rows
- **THEN** scanning yields the table's schema with no rows, not an error

### Requirement: Errors during scan surface as reader errors, not silently dropped

An error encountered while reading a data file during a scan (for example, a missing or corrupt file referenced by the snapshot) SHALL surface through the returned reader as a Go error, and SHALL NOT be silently dropped or produce a truncated result that looks successful.

#### Scenario: Data file unreadable

- **WHEN** a snapshot references a data file that is missing or unreadable at scan time
- **THEN** consuming the scan's reader surfaces a Go error rather than silently omitting that file's rows

### Requirement: No file handle leaks across repeated or concurrent scans

Each scan SHALL manage its own file and catalog-client access such that releasing or fully consuming the returned reader releases any OS resources that scan opened — file handles for the direct `metadata.json` path, and any connections or handles opened resolving the table through a catalog client for the catalog-backed path — independent of other concurrent or subsequent scans of the same provider.

#### Scenario: Concurrent scans

- **WHEN** the same provider is scanned concurrently from multiple goroutines
- **THEN** each scan independently returns the full current-snapshot contents and no scan interferes with another

#### Scenario: Concurrent scans of a catalog-backed provider

- **WHEN** the same catalog-backed provider is scanned concurrently from multiple goroutines
- **THEN** each scan independently resolves the table through the catalog and returns correct results, and no scan's catalog resolution or data-file reads interfere with another's

### Requirement: Provider declares scan pushdown support

The provider SHALL declare scan pushdown support at registration, so that every scan the engine requests carries the query's projection, pushable filter predicates, and limit hint.

#### Scenario: Query with WHERE and column subset reaches the pushdown scan

- **WHEN** a query with a `WHERE` clause and a column subset runs against a registered Iceberg provider
- **THEN** the provider's scan receives the projection and the pushable predicates, and the query result is exactly what the same query would return against a non-pushdown provider with the same data

### Requirement: Projection is honored exactly and prunes column reads

For a scan carrying a projection, the provider SHALL return record batches containing exactly the projected fields, in the requested order, and SHALL restrict the columns read from data files to those needed for the projection and the pushed filters. A nil projection means all fields; an empty projection means zero-column batches that still carry the correct row counts.

#### Scenario: Narrow SELECT returns only requested fields

- **WHEN** a scan requests a projection selecting a subset of the table's fields
- **THEN** the returned batches contain exactly those fields, in the requested order, with values matching the table

#### Scenario: Projection order differs from schema order

- **WHEN** a scan requests a projection whose field order differs from the table's schema order
- **THEN** the returned batches carry the fields in the requested order, not the schema's order

#### Scenario: Filter references a projected-away field

- **WHEN** a scan carries a filter on a field that is not in the projection
- **THEN** the returned batches contain only the projected fields, and the filter is still usable for pruning

#### Scenario: Empty projection

- **WHEN** a scan requests an empty (zero-column) projection of a non-empty table
- **THEN** the returned batches have zero columns and their row counts sum to the number of rows the scan produces

### Requirement: Pushed filters prune via the table format's own metadata

For a scan carrying filter predicates, the provider SHALL translate them into the table format's native filter expressions and apply them to the scan, so that manifests and data files whose partition values and column statistics prove no row can match are never read. Translation SHALL be semantics-preserving: a pushed filter that cannot be translated faithfully — including one that fails to bind against the table's schema — SHALL be dropped in its entirety rather than approximated, and the scan SHALL proceed without it. Dropping a filter SHALL never fail the query.

#### Scenario: Partition pruning skips whole data files

- **WHEN** a scan of a partitioned table carries a predicate on the partition source column that excludes some partitions
- **THEN** data files belonging entirely to excluded partitions are never read, and the query result is identical to a full scan followed by engine-side filtering

#### Scenario: Column-statistics pruning skips data files

- **WHEN** a scan carries the predicate `id > 100` and a data file's column statistics in the manifest report max `id` = 50
- **THEN** that data file is never read, and the query result is unchanged

#### Scenario: Untranslatable filter is dropped, not approximated

- **WHEN** a scan carries a pushed filter the provider cannot translate faithfully into the table format's expression type
- **THEN** the scan runs without that filter, every row that could match is still produced, and the engine's re-applied filter yields the correct query result

### Requirement: Filter pushdown never drops matching rows

Rows that satisfy the pushed filters SHALL always appear in the scan's output. The provider MAY additionally omit rows that cannot satisfy the pushed filters (including exact row-level filtering performed by the underlying table library), and the query result SHALL be identical either way.

#### Scenario: Filtered and unfiltered scans agree

- **WHEN** the same query runs against the same table with filter pushdown active and with it disabled
- **THEN** both produce identical query results

### Requirement: Limit hint is passed to the scan

When a scan carries a limit hint, the provider MAY stop producing rows once it has produced at least that many. The rows produced up to that point SHALL be correct and complete batches.

#### Scenario: Filterless LIMIT stops early

- **WHEN** `SELECT * FROM t LIMIT 3` runs against the provider and the scan receives the limit hint 3
- **THEN** the provider stops producing rows after at least 3, and the query returns exactly 3 rows

### Requirement: Open an Iceberg table via a catalog client

The package SHALL let a Go program construct a table provider by supplying an already-configured Iceberg catalog client (the `catalog.Catalog` interface from `github.com/apache/iceberg-go/catalog`) together with a namespace-qualified table identifier, resolving the table's location and current metadata through that catalog instead of a direct `metadata.json` path. The package SHALL depend only on the `catalog.Catalog` interface, not on any specific catalog implementation (REST, Hive, Glue, SQL, Hadoop, or otherwise) — a caller constructs and configures the concrete catalog client itself and passes it in already built. Construction SHALL fail with a Go error if the identifier's namespace or table does not exist in the catalog, or if the catalog client itself returns an error (for example, a connectivity or authentication failure).

#### Scenario: Valid catalog and identifier

- **WHEN** a program constructs a provider from a working catalog client and the identifier of a table that exists in it
- **THEN** construction succeeds and the resulting provider is ready to register and query

#### Scenario: Table not found

- **WHEN** a program constructs a provider from an identifier whose namespace or table does not exist in the catalog
- **THEN** construction returns a Go error and produces no usable provider

#### Scenario: Catalog client error

- **WHEN** the supplied catalog client returns an error while resolving the identifier (for example, the catalog service is unreachable or rejects the credentials)
- **THEN** construction returns that error as a Go error and produces no usable provider

### Requirement: Catalog-backed schema reflects the table's current schema

A provider constructed via a catalog client SHALL expose the Arrow-equivalent of the Iceberg table's current schema, as resolved through the catalog at construction — the same schema contract the direct `metadata.json` construction path already provides.

#### Scenario: Schema matches the catalog-resolved table

- **WHEN** a provider is constructed via a catalog client from a table with a known current schema
- **THEN** the provider's schema has exactly those fields, with types matching the Arrow-equivalent of each Iceberg field's type

### Requirement: Catalog-backed scans reflect the table's latest committed state

Each scan of a catalog-backed provider SHALL re-resolve the table's current metadata through the catalog, so that a commit made to the table between two scans of the same provider is visible to the later scan. This is unlike the direct `metadata.json` construction path, which always reads the one metadata file it was constructed with.

#### Scenario: A commit between scans is visible to the next scan

- **WHEN** a table is scanned via a catalog-backed provider, a new snapshot is then committed to that table through the catalog, and the same provider is scanned again
- **THEN** the second scan's results reflect the newly committed snapshot, not the snapshot seen by the first scan

### Requirement: Catalog-backed providers declare insert support; metadata-location providers do not

A provider constructed via a catalog client SHALL declare insert support at registration, accepting the append mode (`INSERT INTO`) and the overwrite mode (`INSERT OVERWRITE`) and rejecting the replace mode with an error stating the mode is unsupported; the rejection SHALL fail only that statement and leave the table and session unchanged. A provider constructed from a direct `metadata.json` location SHALL NOT declare insert support — an `INSERT` against it fails with the engine's standard not-writable error without invoking the provider, and its read behavior is unchanged — because such a provider is pinned to one metadata file and has no commit channel through which a write could become the table's current state.

#### Scenario: Insert into a catalog-backed provider

- **WHEN** a program registers a catalog-backed Iceberg provider for table `t` and executes `INSERT INTO t VALUES (...)`
- **THEN** the statement succeeds and reports the number of rows the input delivered

#### Scenario: Insert into a metadata-location provider

- **WHEN** a program executes `INSERT INTO t VALUES (...)` against a provider constructed from a `metadata.json` path
- **THEN** the statement fails with a Go error indicating the table does not support inserts, the table's data is unchanged, and the same session still answers `SELECT` queries against it

#### Scenario: Replace mode is rejected

- **WHEN** a `REPLACE INTO`-mode insert reaches a catalog-backed provider
- **THEN** the statement fails with a Go error stating the mode is unsupported and the table's current snapshot is unchanged

### Requirement: Append commits a new snapshot containing the input rows

A successful append-mode insert SHALL commit, through the catalog, a new table snapshot that contains every row visible before the insert plus every row the insert's input delivered. The commit SHALL be atomic from a reader's perspective: any scan observes either the prior snapshot or the new snapshot in full. An append of zero rows SHALL succeed with a count of 0 and SHALL NOT be required to create a new snapshot. Appending SHALL work for partitioned tables, with rows routed to their partitions according to the table's partition spec.

#### Scenario: Insert then read on the same session

- **WHEN** a table whose current snapshot contains rows A and B receives an append-mode insert of rows C and D, and the same session then executes `SELECT` on the table
- **THEN** the result contains exactly rows A, B, C, and D

#### Scenario: Append to a partitioned table

- **WHEN** an append-mode insert delivers rows spanning multiple partition values of a partitioned table
- **THEN** the statement succeeds and a subsequent scan with a partition-column filter returns exactly the matching old and new rows

### Requirement: Overwrite atomically replaces the table's visible data

A successful overwrite-mode insert SHALL commit, through the catalog, a state in which a scan returns exactly the rows the insert's input delivered — none of the previously visible rows — with the replacement of old data and addition of new data taking effect as one atomic commit, never observable half-applied. An overwrite of zero rows SHALL succeed and leave the table visibly empty (schema intact, zero rows).

#### Scenario: Overwrite then read returns only new rows

- **WHEN** a table whose current snapshot contains rows A and B receives an overwrite-mode insert of row C, and the same session then executes `SELECT` on the table
- **THEN** the result contains exactly row C

#### Scenario: Zero-row overwrite empties the table

- **WHEN** an overwrite-mode insert's input produces no rows
- **THEN** the statement succeeds with a count of 0 and a subsequent scan returns the table's schema with zero rows

### Requirement: A failed insert leaves the table's visible state unchanged

An insert that fails at any point — the input stream erroring mid-consume, a data-file write error, or a commit rejected by the catalog — SHALL fail the statement with a Go error, SHALL NOT be retried by the provider beyond the underlying library's own commit-conflict retry, and SHALL leave the table's current snapshot exactly as it was: a subsequent scan returns the pre-insert contents. Data files written before the failure MAY remain on storage but SHALL NOT be referenced by any snapshot and SHALL never appear in scan results.

#### Scenario: Catalog rejects the commit

- **WHEN** the catalog refuses an insert's commit (for example, a concurrent writer advanced the table and conflict retries are exhausted)
- **THEN** the statement fails with a Go error, a subsequent scan returns the pre-insert contents, and the session remains usable

#### Scenario: Input stream fails mid-insert

- **WHEN** the insert's input stream returns an error after the provider has written some data files
- **THEN** the statement fails with a Go error and no scan ever returns any of the failed insert's rows

### Requirement: Insert schema handling matches the table's schema by name

The provider SHALL write the insert's rows against the table's current schema, resolving the input stream's columns to Iceberg fields by name — the input arrives in the registered schema, whose field names are the table's field names — without depending on Arrow field metadata (such as embedded field IDs) surviving the engine's planning round-trip. The provider's registered schema SHALL NOT change as a result of an insert.

#### Scenario: Post-insert scan schema matches the registered schema

- **WHEN** a session inserts rows into a registered catalog-backed Iceberg table and then queries it
- **THEN** the query succeeds and its result schema is identical to the schema of the same query before the insert

### Requirement: Inserts on one provider instance are serialized; scans stay snapshot-consistent

Concurrent inserts against the same provider instance SHALL be serialized by the provider, so each commit builds on the previous one rather than racing it. A scan running concurrently with an insert SHALL be unaffected: it reads the snapshot it resolved at scan start, in full. Coordination with writers outside the provider instance is the catalog's concern, not the provider's; multi-writer conflict behavior beyond the underlying library's commit-conflict handling is out of scope.

#### Scenario: Two concurrent appends both land

- **WHEN** two append-mode inserts run concurrently against the same catalog-backed provider instance
- **THEN** both statements succeed and a subsequent scan returns the prior rows plus both inserts' rows

#### Scenario: Scan concurrent with an insert

- **WHEN** a scan is consuming the table while an insert into the same provider commits
- **THEN** the scan yields the complete row set of the snapshot it started from, unaffected by the concurrent commit

