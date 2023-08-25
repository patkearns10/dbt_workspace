{{ 
    config( 
        materialized="incremental", 
        incremental_strategy='insert_overwrite' 
        ,partition_by={ 
            "field": "created_at_datetime", 
            "data_type": "DATETIME", 
            "granularity": "day" 
        }) 
}}

select * from {{ ref('incremental_source__datetime') }}