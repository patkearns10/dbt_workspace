-- static
{%- set v_pk_list = ["CORPORATION_CODE"] -%}
{%- set v_house_keeping_column = ["BIW_INS_DTTM","BIW_UPD_DTTM","BIW_BATCH_ID","BIW_MD5_KEY"] -%}
-- derived
{%- set v_all_column_list = edw_get_column_list_new(this) -%}
{%- set v_update_column_list = edw_get_quoted_column_list_new(this, v_pk_list|list + ['BIW_INS_DTTM']|list) -%}
{%- set v_md5_column_list = edw_get_md5_column_list_new(this, v_pk_list|list+ v_house_keeping_column|list ) -%}

-- sql
with

sample_data as (
    select * from {{ ref('stg_onsemi') }}
)

select
    *
    ,
    {{ v_pk_list }} as v_pk_list,
    {{ v_house_keeping_column }} as v_house_keeping_column,
    '{{ v_all_column_list }}' as v_all_column_list,
    {{ v_update_column_list }} as v_update_column_list,
    {{ v_md5_column_list }} as BIW_MD5_KEY
from sample_data
