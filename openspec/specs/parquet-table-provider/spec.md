# parquet-table-provider Specification

## Purpose
Lets a Go program query a local Parquet file directly from SQL, by wrapping it as a `TableProvider` without hand-writing any Parquet-reading code.
## Requirements
### Requirement: Open a local Parquet file as a table provider

The package SHALL let a Go program construct a table provider from the path to a local Parquet file. Construction SHALL fail with a Go error if the file does not exist, is not readable, or is not a valid Parquet file, rather than deferring the failure to the first scan.

#### Scenario: Valid file

- **WHEN** a program constructs a provider from the path to a valid Parquet file
- **THEN** construction succeeds and the resulting provider is ready to register and query

#### Scenario: Missing or invalid file

- **WHEN** a program constructs a provider from a path that does not exist, or a file that is not valid Parquet
- **THEN** construction returns a Go error and produces no usable provider

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

