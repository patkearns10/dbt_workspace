
-- static
{%- set v_pk_list = ["CORPORATION_CODE"]-%}
{%- set v_house_keeping_column = ["BIW_INS_DTTM","BIW_UPD_DTTM","BIW_BATCH_ID","BIW_MD5_KEY"]-%}
-- derived
{%- set v_all_column_list = edw_get_column_list_new(ref('int_onsemi')) -%}
{%- set v_update_column_list = edw_get_quoted_column_list_new(ref('int_onsemi'), v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%- set v_md5_column_list = edw_get_md5_column_list_new(ref('int_onsemi'), v_pk_list|list+ v_house_keeping_column|list ) -%}


{{
    config(
         description = 'Building table Billing_FACT for sales mart '
        ,transient=false
        ,materialized='incremental'
        ,schema ='MART_SALES'
        ,alias='BILLING_FACT'
        ,tags ='MART SALES'
        )
}}

-- sql
with

sample_data as (
    select * from {{ ref('int_onsemi') }}
)

select
    *,
    '{{ v_all_column_list }}' as v_all_column_list_incremental,
    {{ v_update_column_list }} as v_update_column_list_incremental,
    {{ v_md5_column_list }} as v_md5_column_list_incremental
from sample_data

    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where _updated_at > (select max(_updated_at) from {{ this }}) 
    {% endif %}