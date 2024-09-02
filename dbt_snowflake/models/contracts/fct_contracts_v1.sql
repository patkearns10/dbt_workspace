{{
    config(
        materialized='table'
    )
}}

with intermediate as (
    select
        job_id,
        job_type,
        is_job_active,
        created_date,
        completed_date,
        job_duration_days
    from {{ ref('int_contracts', v=1) }}
    {% if is_incremental() %}
        where
            job_id > (select max(job_id) from {{ this }})
    {% endif %}
),

job_names as (
    select
        'abc' as job_type,
        'extract' as job_name
    union all
    select
        'bcd' as job_type,
        'transform' as job_name
    union all
    select
        'def' as job_type,
        'load' as job_name
)

select
    intermediate.job_id,
    intermediate.job_type,
    intermediate.is_job_active,
    intermediate.created_date,
    intermediate.completed_date,
    intermediate.job_duration_days,

    job_names.job_name,

    current_timestamp as updated_at
from intermediate
left join job_names using (job_type)
