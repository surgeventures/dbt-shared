{% materialization fresha_iceberg_incremental, adapter='snowflake', supported_languages=['sql', 'python'] -%}
  {#
    Override of standard incremental materialization for Iceberg REST catalogs.

    Differences from core:
    - Uses CREATE TABLE (columns) + INSERT instead of CREATE TABLE AS SELECT
    - Fallback check for tables that load_relation() misses
  #}

  {% set original_query_tag = set_query_tag() %}

  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set language = model['language'] -%}

  {%- set identifier = model['alias'] -%}
  {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

  {%- set target_relation = api.Relation.create(
    identifier=identifier,
    schema=schema,
    database=database,
    type='table',
    table_format=catalog_relation.table_format,
  ) -%}

  {% set existing_relation = load_relation(this) %}
  {% set existing_relation = dbt_fresha_shared.fresha_iceberg_get_existing_relation(identifier, database, schema, catalog_relation, existing_relation) %}

  {%- set unique_key = config.get('unique_key') -%}
  {% set incremental_strategy = config.get('incremental_strategy') or 'default' %}
  {%- set partition_by = config.get('partition_by') -%}

  {% set grant_config = config.get('grants') %}

  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
    {% set sql_columns = dbt_fresha_shared.fresha_iceberg_get_sql_columns(compiled_code) %}
    {{ dbt_fresha_shared.fresha_iceberg_create_table(target_relation, sql_columns, catalog_relation, partition_by, 'create_table') }}
    {{ dbt_fresha_shared.fresha_iceberg_insert_into(target_relation, compiled_code, language, 'main') }}

  {% elif full_refresh_mode %}
    {% set sql_columns = dbt_fresha_shared.fresha_iceberg_get_sql_columns(compiled_code) %}
    {% if existing_relation %}
      {% call statement('drop_existing') -%}
        DROP TABLE IF EXISTS {{ existing_relation }}
      {%- endcall %}
    {% endif %}
    {{ dbt_fresha_shared.fresha_iceberg_create_table(target_relation, sql_columns, catalog_relation, partition_by, 'create_table') }}
    {{ dbt_fresha_shared.fresha_iceberg_insert_into(target_relation, compiled_code, language, 'main') }}

  {% elif target_relation.table_format != existing_relation.table_format %}
    {% do exceptions.raise_compiler_error(
        "Unable to update the incremental model `" ~ target_relation.identifier ~ "` from `" ~ existing_relation.table_format ~ "` to `" ~ target_relation.table_format ~ "` due to Snowflake limitation. Please execute with --full-refresh to drop the table and recreate in the new catalog.'"
      )
    %}

  {% else %}
    {% set sql_columns = dbt_fresha_shared.fresha_iceberg_get_sql_columns(compiled_code) %}
    {% set tmp_relation = dbt_fresha_shared.fresha_iceberg_make_temp_relation(identifier, schema, database, catalog_relation) %}
    {% do drop_relation_if_exists(tmp_relation) %}
    {{ dbt_fresha_shared.fresha_iceberg_create_table(tmp_relation, sql_columns, catalog_relation, partition_by, 'create_tmp_table') }}
    {{ dbt_fresha_shared.fresha_iceberg_insert_into(tmp_relation, compiled_code, language, 'insert_tmp_table') }}

    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}
    {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {% set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) %}
    {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) %}
    {% set strategy_arg_dict = ({'target_relation': target_relation, 'temp_relation': tmp_relation, 'unique_key': unique_key, 'dest_columns': dest_columns, 'incremental_predicates': incremental_predicates, 'catalog_relation': catalog_relation }) %}

    {%- call statement('main') -%}
      {{ strategy_sql_macro_func(strategy_arg_dict) }}
    {%- endcall -%}

    {% do drop_relation_if_exists(tmp_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks) }}

  {% set target_relation = target_relation.incorporate(type='table') %}

  {% set should_revoke = should_revoke(existing_relation.is_table, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
