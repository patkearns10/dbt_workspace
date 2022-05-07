{% macro variable_update() %}
    {% set disable_models="['some_macro_name']" %}
    {% do log("Updating models to disable: " ~ disable_models, info=true) %}
{% endmacro %}
