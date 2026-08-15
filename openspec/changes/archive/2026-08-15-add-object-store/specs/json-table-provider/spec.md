# json-table-provider Delta

## RENAMED Requirements

- FROM: `### Requirement: Open a local JSON file with a caller-supplied schema`
- TO: `### Requirement: Open a JSON object at a storage location with a caller-supplied schema`

## MODIFIED Requirements

### Requirement: Open a JSON object at a storage location with a caller-supplied schema

The package SHALL let a Go program construct a table provider from the location of a JSON object — a bare local path or a URL whose scheme is registered with the object-store capability — and an `*arrow.Schema` supplied by the caller. A bare local path SHALL behave exactly as a `file://` location. Schema inference is not supported — the caller MUST always supply the schema. The object's top level MUST be a JSON array; each array element MUST be a JSON object decodable against the supplied schema. Construction SHALL fail with a Go error if the location's scheme is not registered, the object does not exist, is not readable, its top level is not a JSON array, or its content does not decode against the supplied schema, rather than deferring the failure to the first scan.

#### Scenario: Valid file matching schema

- **WHEN** a program constructs a provider from the path to a JSON file whose top level is an array of objects compatible with the supplied schema
- **THEN** construction succeeds and the resulting provider's schema is exactly the supplied schema

#### Scenario: Valid object on a registered backend

- **WHEN** a program writes a JSON array-of-objects document to a `mem://` location and constructs a provider from that URL with a matching schema
- **THEN** construction succeeds and querying the registered table returns one row per array element

#### Scenario: Missing or unreadable file

- **WHEN** a program constructs a provider from a location at which no object exists, or from a URL whose scheme has no registered backend
- **THEN** construction returns a Go error and produces no usable provider

#### Scenario: Top level is not an array

- **WHEN** a program constructs a provider from a JSON object whose top-level value is a single object rather than an array
- **THEN** construction returns a Go error and produces no usable provider

#### Scenario: Content incompatible with schema

- **WHEN** a program constructs a provider whose supplied schema types a field as a number but some array element holds a string in that field
- **THEN** construction returns a Go error and produces no usable provider
