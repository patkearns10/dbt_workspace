{{
    config(
        materialized='table'
    )
}}


with

sample_data as (
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
)

select 
    *,
    cast(_updated_at as date) as created_at_date
 from sample_data