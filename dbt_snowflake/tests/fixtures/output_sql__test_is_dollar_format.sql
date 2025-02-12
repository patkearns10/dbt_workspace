with
source as (
    select * from DEVELOPMENT.dbt_pkearns.seed_output__test_is_dollar_format
),

mock as (
    select
        md5(cast(coalesce(cast(order_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(null as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as _pk,
        ----------  ids
        cast(order_id as varchar) as order_id,
        cast(null as varchar) as location_id,
        cast(null as varchar) as customer_id,
        ---------- properties
        order_total,
        tax_paid,
        ---------- timestamps
        cast(null as timestamp_ntz) as ordered_at
    from source
)

select * from mock