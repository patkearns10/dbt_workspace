{{
    config(
        enabled=false
    )
}}

select * from {{ ref('customers_plus_one') }}