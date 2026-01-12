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

## Requirements

- dbt >= 1.0.0
- Snowflake adapter with Iceberg support

## Usage

Once installed, the custom materializations are automatically available in your dbt project.

## Version

Current version: 1.0.0
