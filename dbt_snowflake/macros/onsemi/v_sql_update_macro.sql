{% macro v_sql_update_macro(var_value=4) %}
    
        update DEVELOPMENT.dbt_pkearns.seed__sample_data
        set deleted={{ var_value }}
        where id not in (
            select id from {{ ref('seed__sample_data_status') }}
        )

{% endmacro %}