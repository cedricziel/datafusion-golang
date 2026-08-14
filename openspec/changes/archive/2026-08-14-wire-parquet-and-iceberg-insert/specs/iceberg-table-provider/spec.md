# iceberg-table-provider Delta: wire-parquet-and-iceberg-insert

## ADDED Requirements

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
