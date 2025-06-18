with

source as (

    select * from {{ source('jaffle_shop', 'customers') }}

),

renamed as (

    select

        ----------  ids
        id::text as customer_id,

        ---------- properties
        name as customer_name

    from source

)

select * from renamed