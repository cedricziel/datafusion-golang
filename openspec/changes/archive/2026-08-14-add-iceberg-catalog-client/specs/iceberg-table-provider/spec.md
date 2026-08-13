## ADDED Requirements

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

## MODIFIED Requirements

### Requirement: No file handle leaks across repeated or concurrent scans

Each scan SHALL manage its own file and catalog-client access such that releasing or fully consuming the returned reader releases any OS resources that scan opened — file handles for the direct `metadata.json` path, and any connections or handles opened resolving the table through a catalog client for the catalog-backed path — independent of other concurrent or subsequent scans of the same provider.

#### Scenario: Concurrent scans

- **WHEN** the same provider is scanned concurrently from multiple goroutines
- **THEN** each scan independently returns the full current-snapshot contents and no scan interferes with another

#### Scenario: Concurrent scans of a catalog-backed provider

- **WHEN** the same catalog-backed provider is scanned concurrently from multiple goroutines
- **THEN** each scan independently resolves the table through the catalog and returns correct results, and no scan's catalog resolution or data-file reads interfere with another's
