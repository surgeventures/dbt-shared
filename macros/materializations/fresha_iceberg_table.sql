{% materialization fresha_iceberg_table, adapter='snowflake', supported_languages=['sql', 'python']%}
  {#
    Override of standard table materialization for Iceberg REST catalogs.

    Differences from core:
    - Uses CREATE TABLE (columns) + INSERT instead of CREATE TABLE AS SELECT
    - Partitions temp table first, then fast copy to target (minimizes downtime)
    - Fallback check for tables that adapter.get_relation() misses
  #}

  {% set original_query_tag = set_query_tag() %}

  {%- set identifier = model['alias'] -%}
  {%- set language = model['language'] -%}

  {% set grant_config = config.get('grants') %}

  {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

  {%- set existing_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {% set existing_relation = dbt_fresha_shared.fresha_iceberg_get_existing_relation(identifier, database, schema, catalog_relation, existing_relation) %}

  {%- set target_relation = api.Relation.create(
    identifier=identifier,
    schema=schema,
    database=database,
    type='table',
    table_format=catalog_relation.table_format
  ) -%}

  {{ run_hooks(pre_hooks) }}

  {%- set partition_by = config.get('partition_by') -%}
  {% set sql_columns = dbt_fresha_shared.fresha_iceberg_get_sql_columns(compiled_code) %}

  {# Create temp table with data #}
  {% set tmp_relation = dbt_fresha_shared.fresha_iceberg_make_temp_relation(identifier, schema, database, catalog_relation) %}
  {% do drop_relation_if_exists(tmp_relation) %}
  {{ dbt_fresha_shared.fresha_iceberg_create_table(tmp_relation, sql_columns, catalog_relation, partition_by, 'create_tmp_table') }}
  {{ dbt_fresha_shared.fresha_iceberg_insert_into(tmp_relation, compiled_code, language, 'insert_tmp_table') }}

  {# Drop old target, create new target, copy from temp #}
  {% if existing_relation %}
    {% call statement('drop_existing') -%}
      DROP TABLE IF EXISTS {{ existing_relation }}
    {%- endcall %}
  {% endif %}
  {{ dbt_fresha_shared.fresha_iceberg_create_table(target_relation, sql_columns, catalog_relation, partition_by, 'create_table') }}
  {{ dbt_fresha_shared.fresha_iceberg_insert_into(target_relation, 'SELECT * FROM ' ~ tmp_relation, language, 'main') }}

  {% do drop_relation_if_exists(tmp_relation) %}

  {{ run_hooks(post_hooks) }}

  {% set target_relation = target_relation.incorporate(type='table') %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
