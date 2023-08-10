{{
    config(
        materialized='incremental',
        unique_key=['UNIQUE_ID', 'GENERIC_ID'],
        on_schema_change='append_new_columns',
        merge_exclude_columns = ['version', 'other_col'],
    )
}}

with

sample_data as (
    select
        1 as unique_id,
        1 as generic_id,
        'Pass' as sometimes_bad_id,
        current_timestamp as _updated_at,
        3 as version,
        'xxx' as other_col

    union all

    select
        2 as unique_id,
        1 as generic_id,
        'Fail' as sometimes_bad_id,
        current_timestamp as _updated_at,
        3 as version,
        'xxx' as other_col

    union all

    select
        3 as unique_id,
        1 as generic_id,
        null as sometimes_bad_id,
        current_timestamp as _updated_at,
        3 as version,
        'xxx' as other_col
)

select
    *
from sample_data

    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where _updated_at > (select max(_updated_at) from {{ this }}) 
    {% endif %}