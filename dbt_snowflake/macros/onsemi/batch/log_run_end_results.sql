{% macro log_run_end_results(results) %}

    {% if execute %}
    
        {% do log('Recording model run results in `dbt_meta__run_end_results`.', info=True) %}

        {% for res in results %}
        
            {# 
            /*
              Because results messages can contain single quotes in the error message, we
              replace them with double quotes to avoid errors during the insert.
              Note: to see all results returned, test run on 1 model with:
              log(res, info=True)
            */ 
            #}

            {% if res.message %}
                {% set tidy_message = res.message.replace("'", '"') %}
            {% else %}
                {% set tidy_message = '' %}
            {% endif %}
            
            {% set query -%}
                insert into {{ target.database }}.{{ target.schema }}.dbt_meta__run_end_results values (
                    '{{ invocation_id }}',
                    '{{ res.node.unique_id }}',
                    '{{ res.node.name }}',
                    '{{ res.node.relation_name }}',
                    '{{ res.node.config.materialized }}',
                    '{{ res.status }}', 
                    '{{ tidy_message }}',
                    current_timestamp()
                );
            {%- endset %}
            {% do run_query(query) %}

            
        {% endfor %}
        
    {% endif %}

{% endmacro %}