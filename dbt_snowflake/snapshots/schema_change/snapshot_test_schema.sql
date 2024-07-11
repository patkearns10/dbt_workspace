{% snapshot snapshot_dbt_variables %}
    {{
        config(
            unique_key='id',
            strategy='timestamp',
            updated_at='updated_at_field'
        )
    }}

    select
    dbt_cloud_run_reason_category as id,
    dbt_cloud_run_reason_category,
    dbt_cloud_run_reason,
    current_timestamp as updated_at_field
    from {{ ref('dbt_variables') }}
 
 {% endsnapshot %}