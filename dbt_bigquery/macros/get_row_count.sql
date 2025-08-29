-- extracts no. of rows from bigquery table using information_schema
{% macro get_row_count(database_name, table_schema, table_name, materialized) %}
    {%- do print("get_row_count has started running") -%}

    {% if materialized == 'view' %}
        -- set environment variable to enable or disable count(*).
        {{ count_star(database_name, table_schema, table_name, env_var('DBT_COUNT_STAR', 'ALLOW')) }}
    {% else %}

    --NOTE: commenting this out because it doesnt work in my bigquery project
{#  TODO: Brian add this back in
    {% set regions = ["australia-southeast1", "us-east4", "europe-west2", "asia-southeast1"] %}
    {% for region in regions %}
        {% set table = database_name + ".region-" + region + ".INFORMATION_SCHEMA.TABLE_STORAGE" %}
        {% set query = "SELECT total_rows as row_count FROM `" + table + "` WHERE table_schema='" + table_schema + "' AND table_name='" + table_name + "'" %}
        {% set result = run_query(query) %}
        -- Check if the result is null or empty
        {% if result is not none and result | length > 0 %}
            {% set row_count = result[0]['row_count'] %}
            {{ return(row_count) }}
        {% else %}
#}
            -- set environment variable to enable or disable count(*).
            {{ count_star(database_name, table_schema, table_name, env_var('DBT_COUNT_STAR', 'ALLOW')) }}
{#  TODO: Brian add this back in
        {% endif %}
  
    {% endfor %} 
#}
    {% endif %}
{% endmacro %}


{% macro count_star(database_name, table_schema, table_name, allow_flag) %}
    -- if not valid result is found in any region then use count(*)
    {% if allow_flag == 'ALLOW' %}
        {% set table_name_concat = database_name ~ '.' ~ table_schema ~ '.' ~ table_name %}
        {% set query = "SELECT COUNT(*) as row_count FROM `" + table_name_concat + "`" %}
        {% set result = run_query(query) %}
        {% set row_count = result[0]['row_count'] %}
        {%- do print("get_row_count has finished running") -%}
        {{ return(row_count) }}
    -- env_var('DBT_COUNT_STAR') set to something other than 'ALLOW'
    {% else %}
        {{ return(0) }}
    {% endif %}
{% endmacro %}
