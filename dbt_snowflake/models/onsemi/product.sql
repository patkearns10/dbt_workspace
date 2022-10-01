-- incremental model to bring in:
    -- one time load data
    -- -1 for pk 
    -- stg data 

{{
    config(
        materialized='incremental',
        unique_key="PRODUCT_KEY"
        )
}}

with

-- one time load with an added -1 for product key
one_time_load as (
    {{ dbt_utils.union_relations(
    relations=[
        ref('seed__product__one_time_load'),
        ref('seed__negative_key'),
        ],
    ) }}

),

-- daily updated data
stg_data as (
    select * from {{ ref('seed__product__stg_data') }}
    {%- if is_incremental() %}
    where BIW_UPD_DTTM >= (select max(BIW_UPD_DTTM) from {{ this }})
    {%- endif %}
)

select
    -- using this to only select columns from the one_time_load_table
    {{ dbt_utils.star(from=ref('seed__product__one_time_load')) }}
from one_time_load
    -- after the initial load, we will not need to run this again
    {%- if is_incremental() %}
    where BIW_UPD_DTTM >= (select max(BIW_UPD_DTTM) from {{ this }}) or BIW_UPD_DTTM is null
    {%- endif %}

union all

select * from stg_data