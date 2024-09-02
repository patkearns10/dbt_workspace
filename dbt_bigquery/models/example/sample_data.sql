{{
    config(
        materialized='table',
    )
}}

with

sample_data as (
    select
        1 as generic_id,
        1 as unique_id,
        'Pass' as sometimes_bad_id,
        current_timestamp as _updated_at

    union all

    select
        1 as generic_id,
        2 as unique_id,
        'Fail' as sometimes_bad_id,
        current_timestamp as _updated_at

    union all

    select
        1 as generic_id,
        2 as unique_id,
        null as sometimes_bad_id,
        current_timestamp as _updated_at
)

select
    *
from sample_data
