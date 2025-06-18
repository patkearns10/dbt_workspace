{% macro check_model_full_refresh() %}

{% if execute %}

-- is_incremental(): {{ is_incremental() }}
-- model.config.full_refresh: {{ model.config.full_refresh }}
-- flags.FULL_REFRESH: {{ flags.FULL_REFRESH }}
-- flags.WHICH: {{ flags.WHICH }}

    {% set current_model = graph.nodes[model.unique_id] %}
    {% set full_refresh_config_value = current_model.config.full_refresh %}
    -- full_refresh_config_value: {{ full_refresh_config_value }}
        {% if full_refresh_config_value is none %}
            {% set config_full_refresh = flags.FULL_REFRESH %}
        {% endif %}
    -- config_full_refresh: {{ config_full_refresh }}
    {% do return(config_full_refresh) %}

{% endif %}

{% endmacro %}