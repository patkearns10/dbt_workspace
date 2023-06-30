

select * from `region-us.INFORMATION_SCHEMA.JOBS_BY_USER` jobs
-- CROSS JOIN unnest(jobs.labels) AS lbls
order by creation_time desc