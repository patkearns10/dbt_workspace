select
-- ex: dev, staging, ci, or prod
'{{env_var('DBT_CLOUD_INVOCATION_CONTEXT')}}' as DBT_CLOUD_INVOCATION_CONTEXT,
-- ex: "Development"
'{{env_var('DBT_CLOUD_ENVIRONMENT_NAME')}}' as DBT_CLOUD_ENVIRONMENT_NAME,
-- ex: dev, staging or prod
'{{env_var('DBT_CLOUD_ENVIRONMENT_TYPE')}}' as DBT_CLOUD_ENVIRONMENT_TYPE
