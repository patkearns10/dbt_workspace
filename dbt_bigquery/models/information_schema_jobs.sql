{{
    config(
        materialized='table'
    )
}}

select * from `region-us.INFORMATION_SCHEMA.JOBS` jobs
-- CROSS JOIN unnest(jobs.labels) AS lbls
order by creation_time desc