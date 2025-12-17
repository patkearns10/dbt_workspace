{{
    config(
      materialized='incremental',
      unique_key='id',
    )
}}


with sample_data as (
    -- sample data
    select
        1 as id,
        '2025-01-01 00:00:00.000' as updated_at,
        'x' as new_col

    union all

    select
        2 as id,
        '2025-01-01 00:00:00.000' as updated_at,
        'x' as new_col

    union all

    select
        3 as id,
        '2025-01-02 00:00:00.000' as updated_at,
        'y' as new_col


    union all

    select
        4 as id,
        '2025-01-03 00:00:00.000' as updated_at,
        'z' as new_col

)

select * from sample_data

{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where updated_at > (select max(updated_at) from {{ this }}) 
{% endif %}