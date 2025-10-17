{% macro outside_macro() %}
    {% do print(" -- printing OUTSIDE" ) %}
    {% do log(" -- do log OUTSIDE") %}
    {{ log(" -- log OUTSIDE") }}
    select 1 as outside
    {{ return(inside_macro()) }}
{% endmacro %}
