{% macro edw_get_md5_column_list(param_object_name,param_exclude_column) -%}
        {%- set column_list_quoted = [] -%}
        {%- set md5_list = [] -%}
        {%- set table_desc =  get_columns_in_relation(param_object_name) -%}
        
        {% for column in table_desc %}
                {{ column_list_quoted.append(column.name) }}
        {% endfor %}

        {% for col in param_exclude_column -%}
                {%- if col in column_list_quoted -%}
                        {{ column_list_quoted.remove(col) }}
                {% endif %}
        {% endfor %}

        {% for v_column_name  in column_list_quoted %}
            {%- set v_col_name_add = "'col"+ loop.index|string+"'," -%}
            {{ md5_list.append(v_col_name_add + v_column_name) }}
        {% endfor %}

        {%- set dest_cols_list = md5_list | join(', ') -%}
        {%- set md5_column = 'md5(object_construct ('+dest_cols_list +')::string ) as BIW_MD5_KEY' -%}
    {{ return(md5_column) }}
{% endmacro %}