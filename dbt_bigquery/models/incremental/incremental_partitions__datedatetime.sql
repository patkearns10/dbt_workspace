{{ 
    config( 
        materialized="incremental", 
        incremental_strategy='insert_overwrite' 
        ,partition_by={ 
            "field": "created_at__date_datetime", 
            "data_type": "DATE", 
            "granularity": "day" 
        }) 
}}

-- messing with datetime converted to date as partition

select
*,
date(created_at_datetime) as created_at__date_datetime

from {{ ref('incremental_source__date_datetime') }}