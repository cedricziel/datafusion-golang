## Purpose

Lets a Go program query a local Parquet file directly from SQL, by wrapping it as a `TableProvider` without hand-writing any Parquet-reading code.

## ADDED Requirements

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

Scanning the provider SHALL yield every row in the file, regardless of how many row groups the file contains, as one or more Arrow record batches.

#### Scenario: Multi-row-group file

- **WHEN** a provider backed by a Parquet file with more than one row group is scanned
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
