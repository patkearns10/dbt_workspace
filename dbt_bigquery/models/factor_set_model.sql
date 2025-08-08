
select
    bcusip,
    array_agg(
        struct(
            asof_date,
            type,
            exchange,
            amount
        )
    ) as factor_set

from {{ ref('seed__security_history') }}
group by bcusip