{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        tags='redshift_incremental',
        merge_exclude_columns=['updated_at']
    )
}}

select 
1 as id,
current_timestamp as updated_at

{% if is_incremental() %}
    union all
    
    select
    2 as id,
    current_timestamp as updated_at

    union all
    
    select
    3 as id,
    current_timestamp as updated_at

    union all
    
    select
    4 as id,
    current_timestamp as updated_at

    union all
    
    select
    5 as id,
    current_timestamp as updated_at
{% endif %}