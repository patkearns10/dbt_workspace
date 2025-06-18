
{% macro get_updated_at(graph, target) %}
{# this macro looks for the tag:updated_at, and collects this data, then builds a table of the last time this model ran (dbt_artifacts) and the max updated at value from the column itself #}
{#  Add "{{ get_updated_at() }}" to an on-run-end: block in dbt_project.yml  #}
    
    {%- set tbl -%} {{ target.schema }}.audit_updated_at {%- endset -%}
    {%- set column_tags = [] -%}
    {% if execute %}
        {%- for model in graph.nodes.values() -%}
                {%- for column_name, column in model.columns.items() -%}
                    {%- set column_identifier = model.name ~ '.' ~ column_name -%}
                    {%- set tags = column.tags -%}
                    {%- if 'updated_at' in tags -%}
                        {%- do column_tags.append({ "database": database, "schema": schema, "model": model.name, "column": column_name }) -%}
                    {%- endif -%}
                {%- endfor -%}
        {%- endfor -%}

        create or replace view {{ tbl }} as (
            
            with
            audit_table_output as (

                {% for row in column_tags %}
            
                select * from (
                    with 
                    cte_{{loop.index}} as (
                        select
                            '{{ row.model }}' as model_name,
                            max({{ row.column }}) as max_updated_at
                        from {{ row.database }}.{{ row.schema }}.{{ row.model }}
                    )

                    select
                        '{{ row.database }}' as _database,
                        '{{ row.schema }}' as _schema,
                        '{{ row.model }}' as _model,
                        '{{ row.column }}' as _column,
                        current_timestamp as _audit_table_at,
                        coalesce(tables.last_altered, views.last_altered) as information_schema_last_altered_at,
                        cte_{{loop.index}}.max_updated_at,
                        max(model_executions.run_started_at) as dbt_artifacts_last_ran_at
                    from cte_{{loop.index}} 
                    inner join DEVELOPMENT.dbt_pkearns__dbt_artifact.fct_dbt__model_executions as model_executions
                    on model_executions.name = cte_{{loop.index}}.model_name
                    left join information_schema.tables on tables.table_name = upper(model_executions.name)
                    and tables.table_schema = upper('{{ row.schema }}')
                    left join information_schema.views on views.table_name = upper(model_executions.name)
                    and views.table_schema = upper('{{ row.schema }}')
                    group by 1,2,3,4,5,6,7
                    
                ) as x_{{loop.index}}

            {{ "union all" if not loop.last }}
            
            {%- endfor %}

            )
                
            select * from audit_table_output
        )
    {% endif %}
{% endmacro %}