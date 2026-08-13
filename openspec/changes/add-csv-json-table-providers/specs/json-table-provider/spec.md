## Purpose

Lets a Go program query a local JSON file — whose top level is an array of objects — directly from SQL, by wrapping it as a `TableProvider` against a caller-supplied schema, without hand-writing any JSON-parsing code.

## ADDED Requirements

### Requirement: Open a local JSON file with a caller-supplied schema

The package SHALL let a Go program construct a table provider from the path to a local JSON file and an `*arrow.Schema` supplied by the caller. Schema inference is not supported — the caller MUST always supply the schema. The file's top level MUST be a JSON array; each array element MUST be a JSON object decodable against the supplied schema. Construction SHALL fail with a Go error if the file does not exist, is not readable, its top level is not a JSON array, or its content does not decode against the supplied schema, rather than deferring the failure to the first scan.

#### Scenario: Valid file matching schema

- **WHEN** a program constructs a provider from the path to a JSON file whose top level is an array of objects compatible with the supplied schema
- **THEN** construction succeeds and the resulting provider's schema is exactly the supplied schema

#### Scenario: Missing or unreadable file

- **WHEN** a program constructs a provider from a path that does not exist or is not readable
- **THEN** construction returns a Go error and produces no usable provider

#### Scenario: Top level is not an array

- **WHEN** a program constructs a provider from a JSON file whose top-level value is a single object rather than an array
- **THEN** construction returns a Go error and produces no usable provider

#### Scenario: Content incompatible with schema

- **WHEN** a program constructs a provider whose supplied schema types a field as a number but some array element holds a string in that field
- **THEN** construction returns a Go error and produces no usable provider

### Requirement: Scan reads every array element as a row, in file order

A scan SHALL yield every element of the file's top-level array as a row, in file order. The entire array SHALL be read into a single Arrow record batch — this provider does not chunk or stream partial results across multiple batches.

#### Scenario: Multi-element file

- **WHEN** a provider backed by a JSON file whose top-level array has more than one object is scanned
- **THEN** scanning yields exactly one record batch containing every element as a row, in file order

#### Scenario: Empty array

- **WHEN** a provider is backed by a valid JSON file whose top-level array is empty
- **THEN** scanning yields the provider's schema with no rows, not an error

### Requirement: Decode failures surface as a scan error, not swallowed or silently mistyped

When the file's content cannot be decoded against the provider's schema (a type mismatch, a malformed JSON value, or truncated input), the provider's scan SHALL return a Go error and SHALL NOT return a reader containing partial or incorrect data.

#### Scenario: An element breaks the schema

- **WHEN** a scan's file content contains an array element whose value for a schema field does not match that field's type
- **THEN** the scan returns a Go error, not a reader

### Requirement: No file handle leaks across repeated or concurrent scans

Each scan SHALL manage its own file access such that the file is closed once that scan has finished reading it, independent of other concurrent or subsequent scans of the same provider.

#### Scenario: Repeated scans

- **WHEN** the same provider is scanned many times in sequence
- **THEN** no file descriptors are leaked

#### Scenario: Concurrent scans

- **WHEN** the same provider is scanned concurrently from multiple goroutines
- **THEN** each scan independently returns the full file contents and no scan interferes with another
