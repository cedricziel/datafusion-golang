# catalog-provider Specification

## Purpose
Lets a Go program supply a dynamic catalog to DataFusion: expose schemas and tables discovered at query time, implemented entirely in Go, so SQL can address `catalog.schema.table` against content not known at registration time.
## Requirements
### Requirement: Register a Go-implemented catalog

A session SHALL let a Go program register a named catalog backed entirely by Go code, exposing SQL to tables addressed as `catalog.schema.table`. Registering a second catalog under a name already in use SHALL return an error rather than silently replacing the existing catalog. The engine's default catalog name is not special-cased: registering a Go catalog under it replaces the default catalog, and any tables or functions previously registered via the engine's single-table/function registration mechanisms become unreachable through SQL.

#### Scenario: Register and query catalog.schema.table

- **WHEN** a program registers a Go-implemented catalog named `warehouse` whose `sales` schema reports a table `orders`, then executes `SELECT * FROM warehouse.sales.orders`
- **THEN** the query returns exactly the rows the Go implementation produces for that table

#### Scenario: Duplicate registration

- **WHEN** a program registers a second catalog under a name already registered on the same session
- **THEN** registration returns a Go error and the original catalog remains queryable unchanged

#### Scenario: Registering under the default catalog name replaces it

- **WHEN** a program registers a Go-implemented catalog under the engine's default catalog name
- **THEN** registration succeeds, the default catalog is replaced, and any tables or functions previously registered via the engine's single-table/function registration mechanisms are no longer reachable through SQL

### Requirement: Catalog and schema contents are discovered dynamically

The engine SHALL query a registered catalog's schema names, and a resolved schema's table names, freshly for each query that needs them, rather than caching them at registration time. A schema or table absent when the catalog was registered and later reported by the Go implementation SHALL become queryable without any further registration call.

#### Scenario: Table added after registration becomes queryable

- **WHEN** a registered catalog's schema begins reporting a table `new_table` that it did not report earlier in the program's lifetime
- **THEN** a subsequent query against `catalog.schema.new_table` succeeds without the program re-registering the catalog

#### Scenario: Schema added after registration becomes queryable

- **WHEN** a registered catalog begins reporting a schema name it did not report earlier in the program's lifetime
- **THEN** a subsequent query addressing that schema succeeds without the program re-registering the catalog

### Requirement: Schema and table lookup by name

The engine SHALL resolve a `catalog.schema.table` reference by asking the registered catalog for the named schema and, if found, asking that schema for the named table. If the catalog reports no schema of that name, or the resolved schema reports no table of that name, the query SHALL fail with the engine's standard "not found" error rather than a Go-authored error.

#### Scenario: Query against unknown schema

- **WHEN** a query addresses `catalog.missing_schema.t` against a registered catalog that does not report a schema named `missing_schema`
- **THEN** the query fails with the engine's standard schema-not-found error, and the session remains usable for later queries

#### Scenario: Query against unknown table within a known schema

- **WHEN** a query addresses `catalog.schema.missing_table` against a schema that does not report a table named `missing_table`
- **THEN** the query fails with the engine's standard table-not-found error, and the session remains usable for later queries

### Requirement: Catalog-discovered tables behave like directly-registered tables

A table returned by a registered catalog's schema lookup SHALL be queried using the same table-provider contract as a table registered directly via the engine's single-table registration mechanism: a full, unprojected, unfiltered scan by default, or projection/filter/limit pushdown if the returned implementation declares support for it. The requirements of the table-provider capability apply unchanged to a catalog-discovered table.

#### Scenario: Full scan of a catalog-discovered table

- **WHEN** a query selects all rows from a table resolved through a registered catalog, and that table's implementation does not declare pushdown support
- **THEN** the query returns exactly the rows the implementation produces, with the engine applying any filtering and projection itself

#### Scenario: Pushdown-capable catalog-discovered table receives scan options

- **WHEN** a query with a `WHERE` clause and a column subset runs against a table resolved through a registered catalog whose implementation declares pushdown support
- **THEN** the table's scan receives the projection, pushable filters, and limit hint exactly as a directly-registered pushdown-capable table would

### Requirement: Errors during catalog access surface as query errors

An error returned by the Go catalog implementation while listing schemas, listing tables, or looking up a schema or table by name SHALL abort the query and surface to the caller as a Go error describing the failure. It SHALL NOT crash the process or leave the session unusable for subsequent queries.

#### Scenario: Schema listing fails

- **WHEN** a registered catalog's schema-name listing returns an error while planning a query
- **THEN** the query fails with a Go error, and the session remains usable for later queries

#### Scenario: Table lookup fails

- **WHEN** a resolved schema's table lookup returns an error while planning a query
- **THEN** the query fails with a Go error, and the session remains usable for later queries

### Requirement: Registered catalogs are released with the session

Closing a session SHALL release the engine's references to every catalog registered on it, and to any schema or table objects the engine currently holds as a result of resolving that catalog. A Go catalog, schema, or catalog-discovered table implementation SHALL NOT be invoked after the session that registered it has been closed.

#### Scenario: Close releases catalog references

- **WHEN** a session with a registered catalog is closed
- **THEN** the engine holds no further references to the Go catalog implementation or to any schema/table objects obtained from it, and any goroutines or resources the Go program owns for them remain under its own control

