# parquet-table-provider Delta: wire-parquet-and-iceberg-insert

## ADDED Requirements

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

An insert that fails at any point — the input stream erroring mid-consume, a write error, or any failure before the new file contents are complete — SHALL leave the original file byte-for-byte unchanged and still readable, SHALL fail the statement with a Go error, and SHALL NOT be retried by the provider. The provider SHALL NOT expose a state in which the file is truncated, half-written, or otherwise unreadable, and SHALL remove any temporary file it created for the failed attempt.

#### Scenario: Input stream fails mid-insert

- **WHEN** the insert's input stream returns an error after the provider has consumed part of it
- **THEN** the statement fails with a Go error, the original file's bytes are unchanged, and a subsequent scan returns the file's prior contents

#### Scenario: No stray temporary files after failure

- **WHEN** an insert fails before completion
- **THEN** no temporary file from the failed attempt remains in the file's directory

### Requirement: Schema is stable across writes

The provider's registered schema SHALL NOT change as a result of an insert, and a scan performed after a successful insert SHALL produce record batches in exactly the registered schema — same fields, same types, same order — so that the engine's schema validation of post-insert scans succeeds. Constructing a new provider from the rewritten file SHALL derive the same Arrow schema as the original file.

#### Scenario: Post-insert scan schema matches the registered schema

- **WHEN** a session inserts rows into a registered Parquet table and then queries it
- **THEN** the query succeeds and its result schema is identical to the schema of the same query before the insert

#### Scenario: Rewritten file round-trips its schema

- **WHEN** a new provider is constructed from a file that a previous provider's insert rewrote
- **THEN** the new provider's schema equals the schema the original provider was constructed with

### Requirement: Inserts are serialized; scans see a consistent file

Concurrent inserts against the same provider instance SHALL be serialized by the provider so that each insert's effect is that of running alone — no insert's rows are lost to another insert's rewrite. A scan running concurrently with an insert SHALL observe either the file's pre-insert contents or its post-insert contents in full, never a torn or partially written state. The provider does not coordinate with writers outside the provider instance; external concurrent modification of the file is undefined behavior, unchanged from the read-only provider.

#### Scenario: Two concurrent appends both land

- **WHEN** two append-mode inserts run concurrently against the same provider instance
- **THEN** both statements succeed and a subsequent scan returns the prior rows plus both inserts' rows

#### Scenario: Scan concurrent with an insert

- **WHEN** a scan is consuming the table while an insert into the same provider completes
- **THEN** the scan yields a complete, uncorrupted row set corresponding to either the pre-insert or post-insert file contents
