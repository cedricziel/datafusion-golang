## Purpose

Lets a Go program query a local CSV file directly from SQL, by wrapping it as a `TableProvider`, either against a schema the caller supplies or one inferred from the file itself, without hand-writing any CSV-parsing code.

## ADDED Requirements

### Requirement: Open a local CSV file with an explicit schema

The package SHALL let a Go program construct a table provider from the path to a local CSV file and an `*arrow.Schema` supplied by the caller. The file's first row SHALL be treated as a header row and discarded (used only to validate column count, not names or types). Construction SHALL fail with a Go error if the file does not exist, is not readable, has no header row, or whose header row's column count does not match the schema's field count, rather than deferring the failure to the first scan.

#### Scenario: Valid file matching schema

- **WHEN** a program constructs a provider from the path to a CSV file whose header row has the same number of columns as the supplied schema
- **THEN** construction succeeds and the resulting provider's schema is exactly the supplied schema

#### Scenario: Missing or unreadable file

- **WHEN** a program constructs a provider from a path that does not exist or is not readable
- **THEN** construction returns a Go error and produces no usable provider

#### Scenario: Column count mismatch

- **WHEN** a program constructs a provider with a schema whose field count does not match the CSV file's header column count
- **THEN** construction returns a Go error and produces no usable provider

### Requirement: Open a local CSV file with a schema inferred from its content

The package SHALL let a Go program construct a table provider from the path to a local CSV file with no caller-supplied schema. The provider SHALL derive column names from the file's header row and column types by inferring each column's type from that column's value in the first data row only — not from any subsequent row, and not from the whole file. Construction SHALL fail with a Go error if the file does not exist, is not readable, has no header row, or has a header row but no data row to infer types from.

#### Scenario: Types inferred from first data row

- **WHEN** a program constructs a schema-inferring provider from a CSV file whose header names a column `id` and whose first data row has an integer-looking value in that column
- **THEN** construction succeeds and the provider's schema types that column as an integer type

#### Scenario: Header with no data rows

- **WHEN** a program constructs a schema-inferring provider from a CSV file that has a header row but zero data rows
- **THEN** construction returns a Go error, because there is no row to infer types from

### Requirement: Scan reads every data row

A scan SHALL yield every data row in the file (excluding the header row) as one or more Arrow record batches, in file order.

#### Scenario: Multi-row file

- **WHEN** a provider backed by a CSV file with more than one data row is scanned
- **THEN** the resulting record batches together contain every data row, in file order

#### Scenario: Header-only file with an explicit schema

- **WHEN** a provider constructed with an explicit schema is backed by a CSV file with a header row and zero data rows
- **THEN** scanning yields the provider's schema with no rows, not an error

### Requirement: Values incompatible with the inferred type surface as an error, not silent bad data

For a schema-inferring provider, when a later data row contains a value that cannot be parsed as the type inferred from the first data row, the provider SHALL NOT silently produce an incorrect value for that field. The returned reader's `Err()` SHALL report a non-nil error once such a row has been reached, and the reader SHALL NOT claim to have completed a full, correct scan.

#### Scenario: Later row breaks the inferred type

- **WHEN** a schema-inferring provider's first data row causes a column to be inferred as an integer type, and a later data row holds a non-integer value in that column
- **THEN** the returned reader's `Err()` is non-nil after that row is reached, and the caller can detect the scan did not complete cleanly

### Requirement: No file handle leaks across repeated or concurrent scans

Each scan SHALL manage its own file access such that releasing or fully consuming the returned reader releases any OS resources that scan opened, independent of other concurrent or subsequent scans of the same provider.

#### Scenario: Repeated scans

- **WHEN** the same provider is scanned many times in sequence, each reader fully consumed and released
- **THEN** no file descriptors are leaked

#### Scenario: Concurrent scans

- **WHEN** the same provider is scanned concurrently from multiple goroutines
- **THEN** each scan independently returns the full file contents and no scan interferes with another
