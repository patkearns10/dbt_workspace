{% macro log_run_end_results(results) %}

    {% if execute %}
    
        {{ log('Recording model run results in `dbt_meta__run_end_results`.', info=True) }}

        {% for res in results %}
        
            {# 
            /*
              Because results messages can contain single quotes in the error message, we
              replace them with double quotes to avoid errors during the insert.
            */ 
            #}
            {% set tidy_message = res.message.replace("'", '"') %}
            {% set query -%}
                insert into {{ target.database }}.{{ target.schema }}.dbt_meta__run_end_results values (
                    '{{ invocation_id }}',
                    '{{ res.node.unique_id }}',
                    '{{ res.node.unique_id.split('.')[-1] }}',
                    '{{ res.status }}', 
                    '{{ tidy_message }}',
                    current_timestamp()
                );
            {%- endset %}
            {% do run_query(query) %}

            -- send failure message for incrementals
            {% if res.node.materialized == 'incremental' and res.status == 'error' %}
                -- this only works if you set the job name to something that can be created from the logs, not a random name like `DBT_MART_SALES_BILLING_FACT`
                {%- set v_dbt_job_name = res.node.unique_id.split('.')[-1] -%}
                {{ v_sql_upd_success(v_dbt_job_name) }}
            {% endif %}
            
        {% endfor %}
        
    {% endif %}

{% endmacro %}