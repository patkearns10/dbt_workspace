{% test is_unique_custom(model, column_name, condition='1=1') %}

with validation_errors as (
    select
        {{ column_name }}  as unique_field,
        count(*) as n_records

    from {{ model }}
    where {{ condition }}
    group by unique_field
    having count(*) > 1
)

select *
from validation_errors

{% endtest %}