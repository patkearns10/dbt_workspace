{{
    config(
        materialized='incremental',
        unique_key='unique_id'
    )
}}

with

sample_data as (

    select
        3 as unique_id,
        3 as generic_id,
        'Pass' as some_status,
        current_timestamp as _updated_at

{% if is_incremental() %}

    union all

    select
        3 as unique_id,
        3 as generic_id,
        'Pass' as some_status,
        current_timestamp as _updated_at

{% endif %}

)

select distinct * from sample_data