{{ 
    config( 
        materialized="incremental", 
        incremental_strategy='insert_overwrite' 
        ,partition_by={ 
            "field": "created_at_date", 
            "data_type": "DATE", 
            "granularity": "day" 
        }) 
}}

select * from {{ ref('incremental_source__date') }}

-- if not gated by the if block, this doesnt work in Preview, but does in Run, as long as the table/partition exists:
-- youll get this error:
-- Database Error in rpc request (from remote system.sql) Unrecognized name: _dbt_max_partition at [5:26]
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
where created_at_date >= _dbt_max_partition
{% endif %}
