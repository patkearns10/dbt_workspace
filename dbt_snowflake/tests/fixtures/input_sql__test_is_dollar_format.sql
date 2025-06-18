with 
source as (
    select * from DEVELOPMENT.dbt_pkearns.seed_input__test_is_dollar_format
),

mock as (
    select 
        cast(id as varchar) as id,
        cast(null as varchar) as customer,
        cast(null as timestamp_ntz) as ordered_at,
        cast(null as varchar) as store_id,
        cast(null as number) as subtotal,
        cast(tax_paid as number) as tax_paid,
        cast(order_total as number) as order_total
    from source
)

select * from mock