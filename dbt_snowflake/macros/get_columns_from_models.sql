{% macro get_columns_from_models(model_names) %}
{# Get Column Names #}
{% set model_names=get_model_names() %}
models:
    {%- for model in model_names %}
            {%- set relation=ref(model) %}
            {%- set columns = adapter.get_columns_in_relation(relation) %}
                {%- set column_names = [] %}
                {%- for column in columns %}
                    {%- do column_names.append(column.name) %}
                {%- endfor %}
  - name: {{ model }}
    tests:
      - dbt_expectations.expect_table_columns_to_match_ordered_list:
          column_list: {{ column_names }}
    {%- endfor %}
{%- endmacro %}

{% macro get_model_names() %}    

{# Get model names #}

{%- set model_list = [] %}
  {%- for node in graph.nodes.values() if node.resource_type == "model" %}
    {%- do model_list.append(node.name) %}
  {%- endfor %}
{% do return(model_list) %}

{%- endmacro %}