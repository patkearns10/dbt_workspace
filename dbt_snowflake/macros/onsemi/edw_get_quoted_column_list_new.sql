{%- macro edw_get_quoted_column_list_new(param_object_name,param_exclude_column=[]) -%}
    
    {# -- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}

    {%- set include_cols = [] %}
    {%- set cols = adapter.get_columns_in_relation(param_object_name) -%}
    {%- set param_exclude_column = param_exclude_column | map("lower") | list %}
    {%- for col in cols -%}
        {%- if col.column|lower not in param_exclude_column -%}
            {% do include_cols.append(col.column) %}
        {%- endif %}
    {%- endfor %}

    {{ return(include_cols) }}

{%- endmacro %}
