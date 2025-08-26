{% test not_null_check_identifier_keys(model, column_name, identifier_keys=None) %}
    -- INPUTS:
    -- identifier_keys: {{ identifier_keys }}
    -- model: {{ model }}
    -- column_name: {{ column_name }}
    {% if execute %}
        {% if identifier_keys is none %}
            -- identifier_keys are missing... grab from the model meta config
            {%- set meta = get_model_meta(model) %}
            -- ... found meta: {{ meta }} 
            {%- set identifier_keys = meta.get('identifier_keys') %}
            -- ... using model_identifier_keys: {{ identifier_keys }}
        {% endif %}
    {% endif %}

    select {{ identifier_keys }}
    from {{ model }}
    where {{ column_name }} is null

{% endtest %}
