{% test has_failed_decodes(model, column_name, condition='1=1') %}

with validation_errors as (
    select * from {{ model }}
    where array_length({{ column_name }}) != 0
    and {{ condition }}
)

select * from validation_errors

{% endtest %}