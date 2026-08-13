## Purpose

Lets a Go program query a local-filesystem-backed Apache Iceberg table directly from SQL, by wrapping it as a `TableProvider` without a catalog service or hand-written Iceberg metadata handling.

## ADDED Requirements

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

Scanning the provider SHALL yield every row visible in the table's current snapshot, across all of that snapshot's data files, as one or more Arrow record batches. This change does not support selecting a different snapshot.

#### Scenario: Snapshot spanning multiple data files

- **WHEN** a provider's current snapshot is backed by more than one data file
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
