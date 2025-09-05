{% macro inside_macro() %}
    {% do print("-- printing INSIDE") %}
    {% do log(" -- do log INSIDE") %}
    {{ log("-- log INSIDE") }}
    select 1 as inside
{% endmacro %}
