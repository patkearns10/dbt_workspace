{{ 
    config( 
        materialized="incremental", 
    ) 
}}

    select
        1 as unique_id,
        current_timestamp as _updated_at
    ------------------------
    -- is it incremental?
    ------------------------
    -- {{ is_incremental() }}
    ------------------------
        {% do log("is incremental? " ~ is_incremental() )%}
    ------------------------

{% if is_incremental() %}

    union all

    select
        2 as unique_id,
        current_timestamp as _updated_at

{% endif %}

