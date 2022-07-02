{%- macro edw_get_column_list_new(param_object_name,param_exclude_column=[]) -%}
    {%- set column_list_quoted = edw_get_quoted_column_list_new(param_object_name,param_exclude_column) -%}
    {%- set dest_cols_list = column_list_quoted | join(', ') -%}
    {{ return(dest_cols_list) }}
{%- endmacro %}