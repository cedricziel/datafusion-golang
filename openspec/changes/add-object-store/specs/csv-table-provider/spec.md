# csv-table-provider Delta

## RENAMED Requirements

- FROM: `### Requirement: Open a local CSV file with an explicit schema`
- TO: `### Requirement: Open a CSV object at a storage location with an explicit schema`

- FROM: `### Requirement: Open a local CSV file with a schema inferred from its content`
- TO: `### Requirement: Open a CSV object at a storage location with a schema inferred from its content`

## MODIFIED Requirements

### Requirement: Open a CSV object at a storage location with an explicit schema

The package SHALL let a Go program construct a table provider from the location of a CSV object — a bare local path or a URL whose scheme is registered with the object-store capability — and an `*arrow.Schema` supplied by the caller. A bare local path SHALL behave exactly as a `file://` location. The object's first row SHALL be treated as a header row and discarded (used only to validate column count, not names or types). Construction SHALL fail with a Go error if the location's scheme is not registered, the object does not exist, is not readable, has no header row, or its header row's column count does not match the schema's field count, rather than deferring the failure to the first scan.

#### Scenario: Valid file matching schema

- **WHEN** a program constructs a provider from the path to a CSV file whose header row has the same number of columns as the supplied schema
- **THEN** construction succeeds and the resulting provider's schema is exactly the supplied schema

#### Scenario: Valid object on a registered backend

- **WHEN** a program writes a CSV object to a `mem://` location and constructs a provider from that URL with a matching schema
- **THEN** construction succeeds and querying the registered table returns the object's rows

#### Scenario: Missing or unreadable file

- **WHEN** a program constructs a provider from a location at which no object exists, or from a URL whose scheme has no registered backend
- **THEN** construction returns a Go error and produces no usable provider

#### Scenario: Column count mismatch

- **WHEN** a program constructs a provider with a schema whose field count does not match the CSV object's header column count
- **THEN** construction returns a Go error and produces no usable provider

### Requirement: Open a CSV object at a storage location with a schema inferred from its content

The package SHALL let a Go program construct a table provider from the location of a CSV object — a bare local path or a URL whose scheme is registered with the object-store capability — with no caller-supplied schema. The provider SHALL derive column names from the object's header row and column types by inferring each column's type from that column's value in the first data row only — not from any subsequent row, and not from the whole object. Construction SHALL fail with a Go error if the location's scheme is not registered, the object does not exist, is not readable, has no header row, or has a header row but no data row to infer types from.

#### Scenario: Types inferred from first data row

- **WHEN** a program constructs a schema-inferring provider from a CSV object whose header names a column `id` and whose first data row has an integer-looking value in that column
- **THEN** construction succeeds and the provider's schema types that column as an integer type

#### Scenario: Header with no data rows

- **WHEN** a program constructs a schema-inferring provider from a CSV object that has a header row but zero data rows
- **THEN** construction returns a Go error, because there is no row to infer types from
