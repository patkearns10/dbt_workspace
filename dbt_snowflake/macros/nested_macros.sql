{% macro parent_macro() %}
  {{ child_macro() }}
{% endmacro %}

{% macro child_macro(should_fail=true) %}
    {% if should_fail == true %}
        {{ exceptions.raise_compiler_error("Error! Beep beep boop!") }}
    {% else %}
        select 'test' as some_column
    {% endif %}
{% endmacro %}
