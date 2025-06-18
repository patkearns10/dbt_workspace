{% snapshot SNAP_DIM_CUSTOMERS %}
    {{
        config(
            target_database='development',
            target_schema='snapshots',
            unique_key='C_CUSTKEY',
            strategy='check',
            check_cols='all'
        )
    }}

select * from {{ ref('DIM_CUSTOMERS') }}

{% endsnapshot %}
