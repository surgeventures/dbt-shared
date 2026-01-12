{% macro fresha_iceberg_normalize_data_type(data_type) %}
  {#
    Normalize Snowflake internal data types to valid DDL data types for Iceberg tables.

    When querying Snowflake metadata (e.g., via get_column_schema_from_query),
    it returns internal type representations that are not valid in CREATE TABLE DDL.
    This macro converts them to valid DDL data types.

    Common mappings:
    - FIXED → NUMBER
    - REAL → FLOAT
    - TEXT → VARCHAR

    Args:
      data_type (string): The data type returned from Snowflake metadata

    Returns:
      string: Valid DDL data type
  #}

  {%- set normalized_type = data_type | upper | trim -%}

  {# Handle FIXED with optional precision/scale: FIXED, FIXED(38,0) #}
  {%- if normalized_type.startswith('FIXED') -%}
    {%- if '(' in normalized_type -%}
      {# Extract precision/scale: FIXED(38,0) → NUMBER(38,0) #}
      {%- set params = normalized_type.replace('FIXED', '') -%}
      {{ return('NUMBER' ~ params) }}
    {%- else -%}
      {# Iceberg tables require explicit precision and scale #}
      {{ return('NUMBER(38,0)') }}
    {%- endif -%}

  {# Handle REAL → FLOAT #}
  {%- elif normalized_type == 'REAL' -%}
    {{ return('FLOAT') }}

  {# Handle TEXT with optional length: TEXT, TEXT(16777216) #}
  {%- elif normalized_type.startswith('TEXT') -%}
    {%- if '(' in normalized_type -%}
      {# Extract length: TEXT(16777216) → VARCHAR(16777216) #}
      {%- set params = normalized_type.replace('TEXT', '') -%}
      {{ return('VARCHAR' ~ params) }}
    {%- else -%}
      {{ return('VARCHAR') }}
    {%- endif -%}

  {# No normalization needed - return as-is #}
  {%- else -%}
    {{ return(data_type) }}
  {%- endif -%}

{% endmacro %}


{% macro fresha_iceberg_process_partition_by(catalog_relation) %}
  {#
    Process partition_by from catalog_relation into a comma-separated string for Iceberg tables.

    Args:
      catalog_relation: The catalog relation object containing partition_by

    Returns:
      string or none: Comma-separated partition keys, or none if not partitioned
  #}

  {%- set partition_by_keys = catalog_relation.partition_by -%}
  {%- if partition_by_keys and partition_by_keys is string -%}
    {%- set partition_by_keys = [partition_by_keys] -%}
  {%- endif -%}
  {%- if partition_by_keys -%}
    {{ return(partition_by_keys|join(", ")) }}
  {%- else -%}
    {{ return(none) }}
  {%- endif -%}

{% endmacro %}


{% macro fresha_iceberg_build_table_ddl(relation, sql_columns, catalog_relation) %}
  {#
    Build CREATE ICEBERG TABLE DDL statement for Fresha Iceberg materializations.

    This macro encapsulates the repeated pattern of creating Iceberg tables
    with column definitions, partitioning, external volume, and auto-refresh.

    Args:
      relation: The relation object (target or temp)
      sql_columns: List of column objects with name and data_type attributes
      catalog_relation: The catalog relation object with table metadata

    Returns:
      string: Complete CREATE ICEBERG TABLE DDL statement
  #}

  {%- set partition_by_string = fresha_iceberg_process_partition_by(catalog_relation) -%}

  CREATE ICEBERG TABLE {{ relation }} (
    {%- for column in sql_columns -%}
      {{ adapter.quote(column.name) }} {{ fresha_iceberg_normalize_data_type(column.data_type) }}
      {%- if not loop.last %}, {% endif -%}
    {% endfor -%}
  )
  {%- if partition_by_string %} PARTITION BY ({{ partition_by_string }}){% endif %}
  {%- if catalog_relation.external_volume %} EXTERNAL_VOLUME = '{{ catalog_relation.external_volume }}'{% endif %}
  {%- if catalog_relation.auto_refresh is not none %} AUTO_REFRESH = {{ catalog_relation.auto_refresh }}{% endif %}

{% endmacro %}


{% macro fresha_iceberg_check_table_exists(identifier, database, schema, catalog_relation) %}
  {#
    Check if a Fresha Iceberg table exists via SHOW ICEBERG TABLES.

    Iceberg tables in catalog-linked databases may not be detected by
    standard adapter methods like load_relation() or get_relation().
    This macro explicitly checks using SHOW ICEBERG TABLES.

    Args:
      identifier: The table name
      database: The database name
      schema: The schema name
      catalog_relation: The catalog relation object

    Returns:
      Relation object if table exists, none otherwise
  #}

  {{ log("Checking for Iceberg table via SHOW ICEBERG TABLES", info=False) }}
  {%- call statement('check_iceberg_table', fetch_result=True) -%}
    SHOW ICEBERG TABLES LIKE '{{ identifier }}' IN SCHEMA {{ database }}.{{ schema }}
  {%- endcall -%}

  {%- set iceberg_check_result = load_result('check_iceberg_table') -%}
  {% if iceberg_check_result and iceberg_check_result['data'] and iceberg_check_result['data'] | length > 0 %}
    {{ log("Iceberg table exists in catalog-linked database", info=False) }}
    {%- set existing_relation = api.Relation.create(
      identifier=identifier,
      schema=schema,
      database=database,
      type='table',
      table_format=catalog_relation.table_format
    ) -%}
    {{ return(existing_relation) }}
  {%- else -%}
    {{ return(none) }}
  {%- endif -%}

{% endmacro %}


{% macro fresha_iceberg_create_table(relation, sql_columns, catalog_relation, statement_name='create_table') %}
  {#
    Execute CREATE ICEBERG TABLE statement for Fresha Iceberg materializations.

    Args:
      relation: The relation object to create
      sql_columns: List of column objects with name and data_type attributes
      catalog_relation: The catalog relation object with table metadata
      statement_name: Name for the statement (default: 'create_table')
  #}

  {% call statement(statement_name) -%}
    {{ fresha_iceberg_build_table_ddl(relation, sql_columns, catalog_relation) }}
  {%- endcall %}

{% endmacro %}


{% macro fresha_iceberg_insert_into(relation, compiled_code, language='sql', statement_name='main') %}
  {#
    Execute INSERT INTO statement for Fresha Iceberg materializations.

    Args:
      relation: The relation object to insert into
      compiled_code: The SQL query to insert data from
      language: The language of the compiled code (default: 'sql')
      statement_name: Name for the statement (default: 'main')
  #}

  {%- call statement(statement_name, language=language) -%}
    INSERT INTO {{ relation }}
      {{ compiled_code }}
  {%- endcall -%}

{% endmacro %}
