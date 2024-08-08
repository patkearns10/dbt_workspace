{{
    config(
        materialized = 'table',
        unique_key = 'order_id'
    )
}}

with

source as (

    select * from {{ source('jaffle_shop', 'orders') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['id','customer'])}} as _pk,
        ----------  ids
        id as order_id,
        store_id as location_id,
        customer::text as customer_id,

        ---------- properties
        (order_total / 100.0) as order_total,
        (tax_paid / 100.0) as tax_paid,

        ---------- timestamps
        ordered_at

    from source

)

select * from renamed