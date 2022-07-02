{% macro edw_get_quoted_column_list(param_object_name,param_exclude_column) -%}

        {%- set column_list_quoted = [] -%}
        {%- set table_desc =  get_columns_in_relation(param_object_name) -%}
        
        {% for column in table_desc %}
                {{ column_list_quoted.append(column.name) }}
        {% endfor %}

        {% for col in param_exclude_column -%}
                {%- if col in column_list_quoted -%}
                        {{ column_list_quoted.remove(col) }}
                {% endif %}
        {% endfor %}

    {{ return(column_list_quoted) }}

{% endmacro %}
