{{
    config(
        materialized='view',

    )
}}
{#  sql_header="CALL SYSTEM$WAIT(60);", #}

with
dummy_datas as (
    select * from 
    {{ ref('customers') }}
    JOIN {{ ref('orders') }}
        USING (customer_id)
)

select
    row_number() over (
        partition by customer_name
        order by first_order_date asc
    ) as _order,
    *
from dummy_datas
