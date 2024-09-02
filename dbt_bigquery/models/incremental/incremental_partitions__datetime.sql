{{ 
    config( 
        materialized="incremental", 
        incremental_strategy='insert_overwrite' ,
        partition_by={ 
            "field": "created_at_datetime", 
            "data_type": "DATETIME", 
            "granularity": "day" 
        },
        labels = {'contains_pii': 'no', 'contains_pie': 'unfortunately_not'}
  )
}}

select * from {{ ref('incremental_source__datetime') }}