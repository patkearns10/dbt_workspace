{# Call this with {{ wh_assign_config() }} #}

{% macro wh_assign_config(model=this.name, env=target.name) -%}
    {% set model_env = model ~ '_' ~ env%}

    {# Fill in your table values here #}
    {# In the format "model_env": "warehouse" #}

        {% set wh_dict = {
            "dim_customers_prod" : "DAPL_PROD_WH",
            "dim_customers_dev" : "DAPL_ADHOC_WH",
            "request_prod" : "PROD_WH",
            "request_dev" : "DEV_WH",
            "request_default" : "DEFAULT_WH",
        } -%}

    {{ return(wh_dict[model_env]) }}
{%- endmacro %}
