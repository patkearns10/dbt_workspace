-- Returns a list of the columns from a relation, so you can then iterate in a for loop
{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('stg_customers')) %}
{{ column_names }}
...
{% for column_name in column_names -%}
    {{ column_name }}
{% endfor %}