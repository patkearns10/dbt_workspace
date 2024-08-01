select

{{ log('# DBT_CLOUD_INVOCATION_CONTEXT: '~env_var('DBT_CLOUD_INVOCATION_CONTEXT', "default"), info=True) }}
{{ log('# DBT_CLOUD_ENVIRONMENT_NAME: '~env_var('DBT_CLOUD_ENVIRONMENT_NAME', "default"), info=True) }}
{{ log('# DBT_CLOUD_ENVIRONMENT_TYPE: '~env_var('DBT_CLOUD_ENVIRONMENT_TYPE', "default"), info=True) }}


-- ex: dev, staging, ci, or prod
'{{env_var('DBT_CLOUD_INVOCATION_CONTEXT', "default")}}' as DBT_CLOUD_INVOCATION_CONTEXT,
-- ex: "Development"
'{{env_var('DBT_CLOUD_ENVIRONMENT_NAME', "default")}}' as DBT_CLOUD_ENVIRONMENT_NAME,
-- ex: dev, staging or prod
'{{env_var('DBT_CLOUD_ENVIRONMENT_TYPE', "default")}}' as DBT_CLOUD_ENVIRONMENT_TYPE
