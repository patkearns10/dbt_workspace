{% macro edw_get_md5_column_list_new(param_object_name,param_exclude_column=[]) -%}
    {%- set column_list_quoted = edw_get_quoted_column_list_new(param_object_name,param_exclude_column) -%}
    {%- set md5_list = [] -%}

    {% for v_column_name  in column_list_quoted %}
        {%- set v_col_name_add = "'col"+ loop.index|string+"'," -%}
        {% do md5_list.append(v_col_name_add + v_column_name) %}
    {% endfor %}

        {%- set dest_cols_list = md5_list | join(', ') -%}
        {%- set md5_column = 'md5(object_construct ('+dest_cols_list +')::string )' -%}

    {{ return(md5_column) }}
{% endmacro %}