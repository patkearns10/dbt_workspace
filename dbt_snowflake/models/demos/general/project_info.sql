select
    '{{ dbt_version }}' as dbt_version,
    '{{ project_name }}' as project_name,
    '{{ env_var('DBT_CLOUD_PROJECT_ID', 'dev') }}' as dbt_cloud_project_id,
    '{{ env_var('DBT_CLOUD_JOB_ID', 'dev') }}' as dbt_cloud_job_id,
    '{{ env_var('DBT_CLOUD_RUN_ID', 'dev') }}' as dbt_cloud_run_id
