# iceberg-table-provider Delta

## RENAMED Requirements

- FROM: `### Requirement: Open a local Iceberg table from its metadata location`
- TO: `### Requirement: Open an Iceberg table from its metadata location`

## MODIFIED Requirements

### Requirement: Open an Iceberg table from its metadata location

The package SHALL let a Go program construct a table provider from the location of an Iceberg table's `metadata.json`, without requiring a catalog service. The location MAY be a local filesystem path or a URI on any storage scheme registered with iceberg-go's file-IO registry (e.g. `s3://`, `gs://`, `abfs://` once the corresponding IO implementation is enabled); a local path SHALL keep working with no additional imports or configuration. Construction SHALL fail with a Go error if the location's scheme has no registered IO implementation, or if the metadata cannot be read or parsed as valid Iceberg table metadata.

#### Scenario: Valid table metadata

- **WHEN** a program constructs a provider from the metadata location of a valid local Iceberg table
- **THEN** construction succeeds and the resulting provider is ready to register and query

#### Scenario: Missing or invalid metadata

- **WHEN** a program constructs a provider from a path that does not exist, or a file that is not valid Iceberg table metadata
- **THEN** construction returns a Go error and produces no usable provider

#### Scenario: Cloud scheme without a registered IO implementation

- **WHEN** a program constructs a provider from an `s3://` metadata location in a process that has not enabled a cloud file-IO implementation
- **THEN** construction returns a Go error identifying the unsupported scheme

## ADDED Requirements

### Requirement: Storage properties reach the table's file IO

Constructors SHALL accept optional storage properties (endpoint, region, credentials-related settings, and other iceberg-go file-IO properties) and SHALL pass them through to the file IO used for every metadata and data read the provider performs — and, for catalog-backed providers, for insert-written data and metadata as well. When no properties are supplied, behavior SHALL be unchanged from today. Cloud credentials otherwise resolve through the storage SDK's standard chain, consistent with the object-store capability.

#### Scenario: Properties are honored for metadata-location tables

- **WHEN** a program constructs a provider from an S3-compatible metadata location, supplying endpoint-override properties pointing at a local S3-compatible server
- **THEN** the table's metadata and data reads are served by that endpoint, and scans return the table's rows

#### Scenario: Omitted properties change nothing

- **WHEN** a program constructs a provider from a local metadata location with no storage properties
- **THEN** construction and scans behave identically to a provider constructed before this capability existed
