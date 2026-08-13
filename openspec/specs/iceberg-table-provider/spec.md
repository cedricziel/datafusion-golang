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

Each scan SHALL manage its own file access such that releasing or fully consuming the returned reader releases any OS resources that scan opened, independent of other concurrent or subsequent scans of the same provider.

#### Scenario: Concurrent scans

- **WHEN** the same provider is scanned concurrently from multiple goroutines
- **THEN** each scan independently returns the full current-snapshot contents and no scan interferes with another

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

