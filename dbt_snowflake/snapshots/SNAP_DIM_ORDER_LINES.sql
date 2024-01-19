{% snapshot SNAP_DIM_ORDER_LINES %}
    {{
        config(
            target_database='development',
            target_schema=generate_schema_name('snapshots'),
            unique_key='OL_PK',
            strategy='check',
            check_cols='all'
        )
    }}

select * from {{ ref('DIM_ORDER_LINES') }}
-- include: {{ ref('SNAP_DIM_CUSTOMERS') }}
-- comment for PR

{% endsnapshot %}
