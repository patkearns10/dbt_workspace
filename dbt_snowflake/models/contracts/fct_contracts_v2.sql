{{
    config(
        materialized='table'
    )
}}

with contracts as (
    select * from {{ ref('fct_contracts', v=1) }}
)

select
    job_id,
    job_type,
    is_job_active,
    created_date,
    completed_date,
    job_name,
    updated_at
from contracts
