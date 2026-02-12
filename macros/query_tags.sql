{% macro get_current_query_tag() -%}
    {# Get the current query tag from Snowflake session #}
    {%- set current_query_tag_query -%}
        show parameters like 'query_tag' in session
    {%- endset -%}

    {%- set results = run_query(current_query_tag_query) -%}

    {%- if execute and results -%}
        {%- set query_tag = results.columns[1].values()[0] -%}
        {{ return(query_tag) }}
    {%- endif -%}

    {{ return("") }}
{%- endmacro %}


{% macro set_query_tag() -%}
    {# Build query tag metadata for GitHub Actions context #}

    {% set new_query_tag = {'event': 'dbt_run_gha'} -%}

    {# Add dbt context - always available #}
    {%- do new_query_tag.update(
        project_name=project_name,
        target_name=target.name,
        target_database=target.database,
        target_schema=target.schema,
        invocation_id=invocation_id
    ) -%}

    {# Add model context if available (not in hooks) #}
    {%- if model is defined and model.resource_type in ('model', 'test') -%}
        {%- do new_query_tag.update(
            model_name=model.name,
            resource_type=model.resource_type,
            id=model.unique_id
        ) -%}
    {%- endif -%}

    {# Add GitHub repository context #}
    {%- if env_var('GITHUB_REPOSITORY', False) -%}
        {%- do new_query_tag.update(
            github_repository=env_var('GITHUB_REPOSITORY')
        ) -%}
    {%- endif -%}

    {%- if env_var('GITHUB_SHA', False) -%}
        {%- do new_query_tag.update(
            github_commit_sha=env_var('GITHUB_SHA')
        ) -%}
    {%- endif -%}

    {%- if env_var('GITHUB_REF_NAME', False) -%}
        {%- do new_query_tag.update(
            github_branch=env_var('GITHUB_REF_NAME')
        ) -%}
    {%- endif -%}

    {# Add GitHub workflow context #}
    {%- if env_var('GITHUB_RUN_ID', False) -%}
        {%- do new_query_tag.update(
            github_run_id=env_var('GITHUB_RUN_ID')
        ) -%}
    {%- endif -%}

    {%- if env_var('GITHUB_RUN_NUMBER', False) -%}
        {%- do new_query_tag.update(
            github_run_number=env_var('GITHUB_RUN_NUMBER')
        ) -%}
    {%- endif -%}

    {%- if env_var('GITHUB_WORKFLOW', False) -%}
        {%- do new_query_tag.update(
            github_workflow=env_var('GITHUB_WORKFLOW')
        ) -%}
    {%- endif -%}

    {%- if env_var('GITHUB_JOB', False) -%}
        {%- do new_query_tag.update(
            github_job=env_var('GITHUB_JOB')
        ) -%}
    {%- endif -%}

    {%- if env_var('GITHUB_ACTOR', False) -%}
        {%- do new_query_tag.update(
            github_actor=env_var('GITHUB_ACTOR')
        ) -%}
    {%- endif -%}

    {%- if env_var('GITHUB_EVENT_NAME', False) -%}
        {%- do new_query_tag.update(
            github_event=env_var('GITHUB_EVENT_NAME')
        ) -%}
    {%- endif -%}

    {# Add OpenLineage namespace if available #}
    {%- if env_var('OPENLINEAGE_NAMESPACE', False) -%}
        {%- do new_query_tag.update(
            openlineage_namespace=env_var('OPENLINEAGE_NAMESPACE')
        ) -%}
    {%- endif -%}

    {# Convert to JSON and validate length #}
    {%- set query_tag_json = tojson(new_query_tag) -%}

    {% if query_tag_json|length < 2000 %}
        {% set original_query_tag = get_current_query_tag() %}
        {{ log("Setting query_tag to '" ~ query_tag_json ~ "'. Original query_tag: '" ~ original_query_tag ~ "'", info=True) }}
        {% do run_query("alter session set query_tag = '{}'".format(query_tag_json)) %}
        {{ return(original_query_tag) }}
    {% else %}
        {{ log("WARNING: Query tag length (" ~ query_tag_json|length ~ " chars) exceeds 2000 character limit. Skipping query tag.", info=True) }}
        {{ return(none) }}
    {% endif %}

{% endmacro %}
