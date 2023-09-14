{% macro log_something_hook() %}
        --
        --------------------------
        --$-- This is a hook --$--
        select 1 as some_col
        --------------------------
{% endmacro %}