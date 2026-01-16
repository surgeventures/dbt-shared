{% macro fresha_iceberg_get_sql_columns(compiled_code) %}
  {#
    Get column schema from compiled query.

    Args:
      compiled_code: The SQL query to analyze

    Returns:
      List of column objects with name and data_type
  #}

  {{ return(get_column_schema_from_query(compiled_code, config.get('sql_header', none))) }}

{% endmacro %}


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

  {# Handle TEXT → STRING (Iceberg-compatible) #}
  {%- elif normalized_type.startswith('TEXT') -%}
    {{ return('STRING') }}

  {# Handle CHARACTER VARYING/VARCHAR → STRING (Iceberg-compatible) #}
  {%- elif normalized_type.startswith('CHARACTER VARYING') or normalized_type.startswith('VARCHAR') -%}
    {{ return('STRING') }}

  {# No normalization needed - return as-is #}
  {%- else -%}
    {{ return(data_type) }}
  {%- endif -%}

{% endmacro %}


{% macro fresha_iceberg_build_table_ddl(relation, sql_columns, catalog_relation, partition_by=none) %}
  {#
    Build CREATE ICEBERG TABLE DDL statement for Fresha Iceberg materializations.

    Args:
      relation: The relation object (target or temp)
      sql_columns: List of column objects with name and data_type attributes
      catalog_relation: The catalog relation object with table metadata
      partition_by: Optional partition_by config (string or list)

    Returns:
      string: Complete CREATE ICEBERG TABLE DDL statement
  #}

  {%- set partition_by_string = none -%}
  {%- if partition_by -%}
    {%- set partition_keys = partition_by if partition_by is iterable and partition_by is not string else [partition_by] -%}
    {%- set partition_by_string = partition_keys | join(', ') -%}
  {%- elif catalog_relation.partition_by -%}
    {%- set partition_by_keys = catalog_relation.partition_by -%}
    {%- if partition_by_keys is string -%}
      {%- set partition_by_keys = [partition_by_keys] -%}
    {%- endif -%}
    {%- set partition_by_string = partition_by_keys | join(', ') -%}
  {%- endif -%}

  CREATE ICEBERG TABLE {{ relation }} (
    {%- for column in sql_columns -%}
      {{ adapter.quote(column.name) }} {{ dbt_fresha_shared.fresha_iceberg_normalize_data_type(column.data_type) }}
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

  {%- set unquoted_identifier = identifier.replace('"', '') -%}
  {%- call statement('check_iceberg_table', fetch_result=True) -%}
    SHOW ICEBERG TABLES LIKE '{{ unquoted_identifier }}' IN SCHEMA {{ database }}.{{ schema }}
  {%- endcall -%}

  {%- set iceberg_check_result = load_result('check_iceberg_table') -%}
  {% if iceberg_check_result and iceberg_check_result['data'] and iceberg_check_result['data'] | length > 0 %}
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




{% macro fresha_iceberg_get_existing_relation(identifier, database, schema, catalog_relation, existing_relation) %}
  {#
    Get existing relation with fallback check for Iceberg tables.

    Iceberg tables in catalog-linked databases may not be detected by
    standard adapter methods, so we use SHOW ICEBERG TABLES as fallback.

    Args:
      identifier: The table name
      database: The database name
      schema: The schema name
      catalog_relation: The catalog relation object
      existing_relation: The relation from adapter.get_relation() or load_relation()

    Returns:
      Relation object if table exists, otherwise the input existing_relation
  #}

  {% if not existing_relation %}
    {% set existing_relation = dbt_fresha_shared.fresha_iceberg_check_table_exists(identifier, database, schema, catalog_relation) %}
  {% endif %}
  {{ return(existing_relation) }}

{% endmacro %}


{% macro fresha_iceberg_make_temp_relation(identifier, schema, database, catalog_relation) %}
  {#
    Create a temporary relation object for staging data.

    Args:
      identifier: The base table identifier
      schema: The schema name
      database: The database name
      catalog_relation: The catalog relation object

    Returns:
      Relation object for temp table with __dbt_tmp suffix
  #}

  {%- set quoted = identifier.startswith('"') -%}
  {%- set tmp_identifier = ('"' if quoted else '') ~ identifier.replace('"', '') ~ '__dbt_tmp' ~ ('"' if quoted else '') -%}
  {% set tmp_relation = api.Relation.create(identifier=tmp_identifier, schema=schema, database=database, type='table', table_format=catalog_relation.table_format).incorporate(catalog=catalog_relation.catalog_name, is_table=true) %}
  {{ return(tmp_relation) }}

{% endmacro %}


{% macro fresha_iceberg_create_table(relation, sql_columns, catalog_relation, partition_by=none, statement_name='create_table') %}
  {#
    Create Iceberg table with explicit schema.

    Args:
      relation: The relation object to create
      sql_columns: List of column objects with name and data_type
      catalog_relation: The catalog relation object with table metadata
      partition_by: Optional partition_by config (string or list)
      statement_name: Name for the statement (default: 'create_table')
  #}

  {% call statement(statement_name) -%}
    {{ dbt_fresha_shared.fresha_iceberg_build_table_ddl(relation, sql_columns, catalog_relation, partition_by) }}
  {%- endcall %}

{% endmacro %}


{% macro fresha_iceberg_insert_into(relation, compiled_code, language='sql', statement_name='main') %}
  {#
    Insert data into Iceberg table.

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
