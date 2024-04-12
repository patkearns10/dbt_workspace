{{
    config(
        materialized='table'
    )
}}

with raw_data as (

    select
        1 as job_id,
        'abc' as job_type,
        True as is_job_active,
        TIMEADD(minute, -2000, current_timestamp) as created_date,
        TIMEADD(minute, -4, current_timestamp) as completed_date

    union all
    select
        2 as job_id,
        'bcd' as job_type,
        True as is_job_active,
        TIMEADD(minute, -3000, current_timestamp) as created_date,
        TIMEADD(minute, -23, current_timestamp) as completed_date

    union all
    select
        3 as job_id,
        'def' as job_type,
        True as is_job_active,
        TIMEADD(minute, -8000, current_timestamp) as created_date,
        TIMEADD(minute, -67, current_timestamp) as completed_date

    union all
    select
        3 as job_id,
        'def' as job_type,
        True as is_job_active,
        TIMEADD(minute, -8000, current_timestamp) as created_date,
        TIMEADD(minute, -67, current_timestamp) as completed_date

)

select
    job_id,
    job_type,
    is_job_active,
    created_date,
    completed_date,
    datediff(minute, created_date, completed_date) / 60 / 24 as job_duration_days
from 
raw_data