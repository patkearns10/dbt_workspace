{{
    config(
        materialized='incremental',
        unique_key=['generic_id', 'unique_id', 'sometimes_bad_id']
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

    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where _updated_at > (select max(_updated_at) from {{ this }}) 
    {% endif %}