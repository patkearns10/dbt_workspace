{% macro edw_get_column_list(param_object_name,param_exclude_column) -%}
    {# -- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}

        {%- if param_object_name.name == 'request' -%}
                {{ return([]) }}
        {% else %}
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

                {%- set dest_cols_list = column_list_quoted | join(', ') -%}

        {{ return(dest_cols_list) }}
        {%- endif %}
{% endmacro %}


