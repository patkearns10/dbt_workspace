-- incremental model to bring in:
    -- one time load data
    -- -1 for pk 
    -- stg data 

{{
    config(
        materialized='incremental',
        unique_key="CORPORATION_KEY",
        enabled=false
        )
}}

with

-- one time load with an added -1 for product key
one_time_load as (
    {{ dbt_utils.union_relations(
    relations=[
        ref('seed__corporation__one_time_load'),
        ref('seed__negative_key'),
        ],
    ) }}

),

-- daily updated data
stg_data as (
    select * from {{ ref('seed__corporation__stg_data') }}
    {%- if is_incremental() %}
    where snapshot__date >= (select max(snapshot__date) from {{ this }})
    {%- endif %}
)

select
    -- using this to only select columns from the one_time_load_table
    {{ dbt_utils.star(from=ref('seed__corporation__one_time_load')) }}
from one_time_load
    -- after the initial load, we will not need to run this again
    {%- if is_incremental() %}
    where snapshot__date >= (select max(snapshot__date) from {{ this }}) or snapshot_date is null
    {%- endif %}

union all

select * from stg_data
