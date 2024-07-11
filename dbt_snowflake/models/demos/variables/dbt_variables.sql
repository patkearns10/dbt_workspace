select
    '{{ env_var('DBT_CLOUD_RUN_REASON_CATEGORY', 'default') }}' as dbt_cloud_run_reason_category,
    '{{ env_var('DBT_CLOUD_RUN_REASON', 'default') }}' as dbt_cloud_run_reason