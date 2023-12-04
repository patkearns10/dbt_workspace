{% snapshot snapshot_hard_deletes %}

{{
    config(
        target_database='development',
        target_schema='snapshots',
        unique_key='id',
        strategy='timestamp',
        updated_at='local_tz',
        invalidate_hard_deletes=True
    )
}}

select 
    1 as id,
    current_timestamp() as local_tz  -- LA

-- union all

-- select 
--     2 as id,
--     current_timestamp() as local_tz  -- LA



{% endsnapshot %}