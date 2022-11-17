{% macro check_against_information_schema(target_sources='raw', target_database='raw') %}
    {% set sources=get_sources(target_sources) %}

    {%- set information_schema_query %}
        with
        information_schema_relations as (
            select
                concat_ws('.', table_schema, table_name) as relation_name
            from {{ target_database }}.information_schema.tables 
            {# -- where table_schema ilike '{{ target_schema }}%' #}
        )

        {# get count of sources in target database #}
        select count(*) from information_schema_relations   --todo remove +1
        where
        relation_name in
            (
                {%- for source in sources -%}
                    '{{ source.upper() }}'
                    {%- if not loop.last -%},{% endif %}
                {%- endfor -%}
            )
    {% endset %}

    {%- set results = run_query(information_schema_query) %}

    {% if execute %}
        {% set information_schema_count = results.columns[0].values()[0] %}
    {% else %}
        {% set information_schema_count = [] %}
    {% endif %}

{# If the count of current sources matches count in target database, check passes #}
    {% if sources|length == information_schema_count %}
        {{ log("Test passed: Found " ~ sources|length ~ " sources from the `" ~ target_sources ~ "` database and " ~ information_schema_count ~ " matching sources in the `" ~ target_database ~ "` database", info=True) }}

    {% else %}
{# If the count of sources does not match in both databases, check fails #}
        {%- set find_missing_sources_query %}
            with
            information_schema_relations as (
                select
                    concat_ws('.', table_schema, table_name) as relation_name
                from {{ target_database }}.information_schema.tables 
                {# -- where table_schema ilike '{{ target_schema }}%' #}
            )
            select relation_name from information_schema_relations
            where
            relation_name in
                (
                    {%- for source in sources -%}
                        '{{ source.upper() }}'
                        {%- if not loop.last -%},{% endif %}
                    {%- endfor -%}
                )
        {% endset %}

        {%- set results = run_query(find_missing_sources_query) %}

        {% if execute %}
            {% set found_sources = results.columns[0].values() %}

        {# check if current sources are found within target sources, if not, list them #}
        {% set missing_sources=[] %}
        {%- for source in sources -%}
            {% if source.upper() not in found_sources %}
                {% do missing_sources.append(source) %}
            {% endif %}
        {%- endfor -%}

        {{ log("Test FAILED: Missing sources in target environment: " ~ missing_sources , info=True) }}
        {{ missing_sources }}
        
        {{ exceptions.raise_compiler_error("Sources in `" ~ target_sources ~ "` database do not exist in the `" ~ target_database ~ "` database.") if execute }}
        {% else %}
            {{ log("Test FAILED", info=True) }}
        {% endif %}

    {% endif %}
{% endmacro %}
