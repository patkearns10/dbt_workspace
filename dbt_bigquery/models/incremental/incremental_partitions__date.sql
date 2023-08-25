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
