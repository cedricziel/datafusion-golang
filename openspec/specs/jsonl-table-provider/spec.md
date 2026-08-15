# jsonl-table-provider Specification

## Purpose
Lets a Go program query a local JSON Lines (newline-delimited JSON) file directly from SQL, by wrapping it as a `TableProvider` against a caller-supplied schema, without hand-writing any JSON-parsing code.
## Requirements
### Requirement: Open a JSON Lines object at a storage location with a caller-supplied schema

The package SHALL let a Go program construct a table provider from the location of a JSON Lines object — a bare local path or a URL whose scheme is registered with the object-store capability — and an `*arrow.Schema` supplied by the caller. A bare local path SHALL behave exactly as a `file://` location. Schema inference is not supported — the caller MUST always supply the schema. Construction SHALL fail with a Go error if the location's scheme is not registered, the object does not exist, is not readable, or if its first JSON object cannot be decoded against the supplied schema, rather than deferring the failure to the first scan.

#### Scenario: Valid file matching schema

- **WHEN** a program constructs a provider from the path to a JSON Lines file whose first object's fields are compatible with the supplied schema
- **THEN** construction succeeds and the resulting provider's schema is exactly the supplied schema

#### Scenario: Valid object on a registered backend

- **WHEN** a program writes a JSON Lines document to a `mem://` location and constructs a provider from that URL with a matching schema
- **THEN** construction succeeds and querying the registered table returns one row per line

#### Scenario: Missing or unreadable file

- **WHEN** a program constructs a provider from a location at which no object exists, or from a URL whose scheme has no registered backend
- **THEN** construction returns a Go error and produces no usable provider

#### Scenario: First object incompatible with schema

- **WHEN** a program constructs a provider whose supplied schema types a field as a number but the file's first JSON object holds a string in that field
- **THEN** construction returns a Go error and produces no usable provider

### Requirement: Scan reads every JSON object as a row, in file order

A scan SHALL yield every JSON object in the file as a row, across one or more Arrow record batches, in file order. Each line (or, more precisely, each consecutive JSON value in the stream) SHALL become exactly one row.

#### Scenario: Multi-object file

- **WHEN** a provider backed by a JSON Lines file with more than one object is scanned
- **THEN** the resulting record batches together contain every object as a row, in file order

#### Scenario: Empty file

- **WHEN** a provider is backed by a valid, empty JSON Lines file
- **THEN** scanning yields the provider's schema with no rows, not an error

### Requirement: Decode failures surface as reader errors, not swallowed or silently mistyped

When a row's JSON value cannot be decoded against the provider's schema (a type mismatch, a malformed JSON value, or truncated input), the provider SHALL NOT silently substitute an incorrect or null value for that row. The returned reader's `Err()` SHALL report a non-nil error once such a row is reached, and rows already produced before that point SHALL remain correct.

#### Scenario: A later row breaks the schema

- **WHEN** a scan reaches a JSON object whose value for a schema field does not match that field's type
- **THEN** the returned reader's `Err()` is non-nil after that row is reached, and the caller can detect the scan did not complete cleanly

### Requirement: No file handle leaks across repeated or concurrent scans

Each scan SHALL manage its own file access such that releasing or fully consuming the returned reader releases any OS resources that scan opened, independent of other concurrent or subsequent scans of the same provider.

#### Scenario: Repeated scans

- **WHEN** the same provider is scanned many times in sequence, each reader fully consumed and released
- **THEN** no file descriptors are leaked

#### Scenario: Concurrent scans

- **WHEN** the same provider is scanned concurrently from multiple goroutines
- **THEN** each scan independently returns the full file contents and no scan interferes with another

