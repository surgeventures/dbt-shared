# dbt-fresha-shared

Shared dbt macros and materializations for Fresha data engineering projects.

## Overview

This package contains reusable dbt macros that can be shared across multiple Fresha dbt projects. It includes custom materializations and helper functions for working with Snowflake Iceberg tables.

## Installation

Add to your `packages.yml`:

```yaml
packages:
  - git: "git@github.com:yourorg/dbt-shared.git"
    revision: v1.0.0
```

Then run:

```bash
dbt deps
```

## Contents

- **materializations/** - Custom dbt materializations for Iceberg tables
- **iceberg/** - Helper macros for Iceberg table operations

## Why Custom Materializations?

As of now, we require custom materializations due to the following limitations:
1. Snowflake Iceberg REST catalog-linked databases
- They don't support `CREATE OR REPLACE ICEBERG TABLE` → must use `DROP TABLE` + `CREATE TABLE`)
2. Lakekeeper
- It doesn't support `CREATE ICEBERG TABLE AS (SELECT ...)` → must use `CREATE TABLE` + `INSERT`)
   ```
   SQL Execution Error: Failed while committing transaction to external catalog. Error:'SQL Execution Error: Table does not exist in the external catalog. Details: 'Error getting tabular from catalog''
   ```
3. Standard dbt adapter methods
- The adapter doesn't detect iceberg tables in catalog-linked databases → must use `SHOW ICEBERG TABLES`

## Materializations

### `fresha_iceberg_table`
Full refresh table materialization for Iceberg tables.

Differences from core:
- uses `CREATE TABLE (columns)` + `INSERT` instead of `CREATE TABLE AS SELECT`
- Partitions temp table first, then fast copy to target (minimizes downtime)
- Fallback check for tables that `adapter.get_relation()` misses

### `fresha_iceberg_incremental`
Incremental materialization for Iceberg tables.

Differences from core:
- Uses `CREATE TABLE (columns)` + `INSERT` instead of `CREATE TABLE AS SELECT`
- Fallback check for tables that `load_relation()` misses

## Helper Macros

Helper macros are documented inline in `macros/iceberg/helpers.sql` with detailed args/returns information.

## Versioning

This project follows [Semantic Versioning](https://semver.org/) (MAJOR.MINOR.PATCH).

This is handled automatically by the CI/CD pipeline.

### Using in Your Project

Always pin to a specific version tag in `packages.yml`:

```yaml
# dbt-x/packages.yml
packages:
  - git: "https://github.com/surgeventures/dbt-shared.git"
    revision: v1.0.0
```
> [!NOTE]
> At the moment, to simplify the dbt Cloud deployment, this repo is publicly accessible.

### Release Process

Releases are automated:

1. Update `version` in `dbt_project.yml`
2. Merge to `main`
3. GitHub Actions auto-creates the tag and release
