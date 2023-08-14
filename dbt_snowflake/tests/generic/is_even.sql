{% test is_even(model, column_name, to) %}

with validation as (

    select
        {{ column_name }} as even_field

    from {{ model }} m
    left join
    {{ to }} c
    on m.customer_id = c.customer_id

),

validation_errors as (

    select
        even_field

    from validation
    -- if this is true, then even_field is actually odd!
    where (even_field % 2) = 1

)

select *
from validation_errors

{% endtest %}