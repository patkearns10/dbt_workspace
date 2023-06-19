{% macro log_something() %}
    {% if model.resource_type != 'test' %}
        --$-- This is not a test! --$--
    {% endif %}
{% endmacro %}