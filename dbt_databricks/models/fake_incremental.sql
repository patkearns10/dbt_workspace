{{
    config(
        materialized='incremental',
        on_schema_change='sync_all_columns'
    )
}}

select
    id,
    _timestamp,
    dwh_valid_from
from {{ ref('fake_stg') }}

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where dwh_valid_from >= (select max(this.dwh_valid_from) from {{ this }} as this) 
{% endif %}
