{% macro generate_uuid() %}
    {% if execute %}
        {% set query = "SELECT GENERATE_UUID() AS uuid" %}
        {% set result = run_query(query) %}
        {% set uuid = result[0]['uuid'] %}
        {{ return(uuid) }}
    {% endif %}
    select 1 -- dummy SQL for parsing stage
{% endmacro %}