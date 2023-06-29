{{
    config(
        materialized='table'
    )
}}

with stg as (
    select
        job_id,
        job_type,
        is_job_active,
        created_date,
        completed_date,
        job_duration_days
    from {{ ref('stg_contracts', v=1) }} 
)

select
    *,
    current_timestamp as updated_at
from stg
