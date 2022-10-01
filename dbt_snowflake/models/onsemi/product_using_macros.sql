-- incremental model to bring in:
    -- one time load data
    -- -1 for pk 
    -- stg data 
-- using macros instead

{{
    config(
        materialized='incremental',
        unique_key="PRODUCT_KEY"
        )
}}

with

{{ one_time_load(from=ref('seed__product__one_time_load')) }}

-- daily updated data
stg_data as (
    select * from {{ ref('seed__product__stg_data') }}
    {%- if is_incremental() %}
    where BIW_UPD_DTTM >= (select max(BIW_UPD_DTTM) from {{ this }})
    {%- endif %}
)

select * from one_time_load
    -- after the initial load, this should return 0 records
    {%- if is_incremental() %}
    where BIW_UPD_DTTM >= (select max(BIW_UPD_DTTM) from {{ this }}) or BIW_UPD_DTTM is null
    {%- endif %}

union all

select * from stg_data