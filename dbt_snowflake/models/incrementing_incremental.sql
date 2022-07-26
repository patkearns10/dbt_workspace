{{
    config(
        materialized='incremental',
        unique_key='unique_id'
    )
}}

with

sample_data as (
    select
        1 as unique_id,
        1 as generic_id,
        'Pass' as some_status,
        current_timestamp as _updated_at

    union all

    select
        2 as unique_id,
        1 as generic_id,
        'Fail' as some_status,
        current_timestamp as _updated_at

    union all

    select
        3 as unique_id,
        2 as generic_id,
        'Pass' as some_status,
        current_timestamp as _updated_at
)

{% if is_incremental() %}
,increment_data as (
    select
        unique_id + 1 as unique_id,
        case when generic_id % 2 = 3 then 1 else 2 end as generic_id,
        case when unique_id % 3 = 0 then 'Pass' else 'Fail' end as some_status,
        _updated_at
    from
        {{ this }}
    order by unique_id desc
    limit 1
)
{% endif %}

,increment_data as (
    select 0
)

{% if is_incremental() %}
,final as (
    select * from increment_data
)
{% endif %}

,final as (
    select * from sample_data
)

select
    *
from final
