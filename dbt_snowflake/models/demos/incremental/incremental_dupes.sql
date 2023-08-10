{{
    config(
        materialized='incremental',
        unique_key='unique_id'
    )
}}

with

sample_data as (


{% if is_incremental() %}

    select
        3 as unique_id,
        3 as generic_id,
        'Pass' as some_status,
        current_timestamp as _updated_at

    union all

    select
        3 as unique_id,
        3 as generic_id,
        'Pass' as some_status,
        current_timestamp as _updated_at

{% endif %}

)

select distinct * from sample_data