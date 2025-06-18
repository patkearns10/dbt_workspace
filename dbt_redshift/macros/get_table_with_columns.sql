{% macro get_table_with_columns(ref_table) -%}

{%- set column_names = adapter.get_columns_in_relation(ref_table) -%}
with source as (

select * from {{ ref_table }}

),

renamed as (

select
-- column_names variable: {{ column_names }}
{%- for col in column_names %}
    {{ col.name }} {% if not loop.last %},{% endif %}
{%- endfor %}
from source

)

select * from renamed
{%- endmacro %}