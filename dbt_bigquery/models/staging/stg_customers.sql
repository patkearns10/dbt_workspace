{{
    config(
        materialized='ephemeral'
    )
}}

select
    id as customer_id,
    first_name,
    last_name,
    concat(first_name, ' ', last_name) as customer_name

from {{ source('jaffle_shop', 'customers') }}