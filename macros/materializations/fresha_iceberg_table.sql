{% materialization fresha_iceberg_table, adapter='snowflake', supported_languages=['sql', 'python']%}
  {#
    Fresha Iceberg Table Materialization
    - Always replaces the table (like standard table materialization)
    - Uses 2-step CREATE + INSERT (required for Iceberg catalog-linked databases)
    - Detects existing Iceberg tables that adapter.get_relation() misses

    Debug logs: dbt run --debug --select <model_name>
  #}

  {% set original_query_tag = set_query_tag() %}

  {%- set identifier = model['alias'] -%}
  {%- set language = model['language'] -%}

  {% set grant_config = config.get('grants') %}

  {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}
  {{ log("Catalog relation: " ~ catalog_relation, info=False) }}

  {%- set existing_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

  {# Iceberg tables in catalog-linked databases may not be detected by adapter.get_relation() #}
  {% if not existing_relation %}
    {% set existing_relation = dbt_fresha_shared.fresha_iceberg_check_table_exists(identifier, database, schema, catalog_relation) %}
  {% endif %}

  {%- set target_relation = api.Relation.create(
	identifier=identifier,
	schema=schema,
	database=database,
	type='table',
	table_format=catalog_relation.table_format
   ) -%}

  {{ run_hooks(pre_hooks) }}

  {# Create temp relation for staging data (minimizes downtime) #}
  {% set tmp_relation = make_temp_relation(target_relation).incorporate(type='table', catalog=catalog_relation.catalog_name, is_table=true) %}
  {{ log("Using temp relation: " ~ tmp_relation, info=False) }}

  {% set sql_columns = get_column_schema_from_query(compiled_code, config.get('sql_header', none)) %}
  {{ log("Retrieved " ~ sql_columns|length ~ " columns from query", info=False) }}

  {# Step 1: Create temp table and insert data (while old table still available) #}
  {{ log("Creating temp table and inserting data", info=False) }}
  {{ dbt_fresha_shared.fresha_iceberg_create_table(tmp_relation, sql_columns, catalog_relation, 'create_tmp_table') }}
  {{ dbt_fresha_shared.fresha_iceberg_insert_into(tmp_relation, compiled_code, language, 'insert_tmp_table') }}

  {# Step 2: Drop old table (downtime starts) #}
  {% if existing_relation %}
    {{ log("Dropping existing relation", info=False) }}
    {% call statement('drop_existing') -%}
      DROP TABLE IF EXISTS {{ existing_relation }}
    {%- endcall %}
  {% endif %}

  {# Step 3: Create new table with same schema #}
  {{ log("Creating target table", info=False) }}
  {{ dbt_fresha_shared.fresha_iceberg_create_table(target_relation, sql_columns, catalog_relation, 'create_target') }}

  {# Step 4: Copy data from temp table (fast operation, downtime ends) #}
  {{ log("Copying data from temp table", info=False) }}
  {%- call statement('main') -%}
    INSERT INTO {{ target_relation }}
      SELECT * FROM {{ tmp_relation }}
  {%- endcall %}

  {{ log("Dropping temp table", info=False) }}
  {% do drop_relation_if_exists(tmp_relation) %}

  {{ run_hooks(post_hooks) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
