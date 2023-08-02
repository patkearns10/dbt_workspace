{% test is_not_null_custom(model, column_name, header) %}

{{ config(sql_header = header) }}

with validation_errors as (

    select {{ column_name }} 
      from {{ model }}
     where {{ column_name }} is null
)

select *
from validation_errors

{% endtest %}
