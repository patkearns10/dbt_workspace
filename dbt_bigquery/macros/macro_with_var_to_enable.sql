{% macro macro_with_var_to_enable() %}
    {% if var('allow_count', 0) == 0 %}
        select 1 as use_information_schema
    {% else %}
        select 1 as select_count_star
    {% endif %}
{% endmacro %}