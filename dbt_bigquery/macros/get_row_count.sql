-- extracts no. of rows from bigquery table using information_schema
{% macro get_row_count(database_name, table_schema, table_name) %}
    {# {% set regions = ["australia-southeast1", "us-east4", "europe-west2", "asia-southeast1"] %}
    {% for region in regions %}
        {% set table = database_name + ".region-" + region + ".INFORMATION_SCHEMA.TABLE_STORAGE" %}
        {% set query = "SELECT total_rows as row_count FROM `" + table + "` WHERE table_schema='" + table_schema + "' AND table_name='" + table_name + "'" %}
        {% set result = run_query(query) %}
        -- Check if the result is null or empty
        {% if result is not none and result | length > 0 %}
            {% set row_count = result[0]['row_count'] %}
            {{ return(row_count) }}
        {% else %}
            -- Continue to the next region if no result is found
            {% continue %}
        {% endif %}
    {% endfor %} #}
 
    -- if not valid result is found in any region then use count(*)
    {% set table_name_concat = database_name ~ '.' ~ table_schema ~ '.' ~ table_name %}
    {% set query = "SELECT COUNT(*) as row_count FROM `" + table_name_concat + "`" %}
    {% set result = run_query(query) %}
    {% set row_count = result[0]['row_count'] %}
    {{ return(row_count) }}
 
 
    -- If nil value then return default value
    {{ return(0) }}
{% endmacro %}