{% macro variable_update() %}
    {% set disable_models="['some_macro_name']" %}
    {% do log("Updating disable_models: " ~ disable_models, info=true) %}
{% endmacro %}
