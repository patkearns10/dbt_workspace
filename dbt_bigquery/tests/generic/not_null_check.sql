{% test not_null_check(model, column_name, identifier_keys) %}
 
    with cte as (
        select
            {{ column_name }},
            count(*) as n_records
        from {{ model }}
        where {{ column_name }} IS NOT NULL
        group by {{ column_name }}
        having count(*) > 0
    )
 
    select {{ identifier_keys }}
        from {{ model }}
    where {{ column_name }}
    IN (
        SELECT
            {{ column_name }}
        FROM
            cte
        WHERE {{ column_name }} is not NULL
    )
 
{% endtest %}
