{% snapshot snapshot_dupe %}

{{
    config(
      target_database='development',
      target_schema='snapshots',
      unique_key='some_value',
      strategy='check',
      check_cols=['id', 'some_value'],
      enabled=False
    )
}}

-- this will fail because of dupes
-- 03:54:07  Database Error in snapshot snapshot_dupe (snapshots/snapshot_dupe.sql)
-- 100090 (42P18): Duplicate row detected during DML action

select
 1 as id,
 3 as some_value,
 TIMEADD(minute, -4, current_timestamp) as updated_at

union all

select
 1 as id,
 3 as some_value,
 TIMEADD(minute, -4, current_timestamp) as updated_at

union all

select
 2 as id,
 3 as some_value,
 TIMEADD(minute, -4, current_timestamp) as updated_at

{% endsnapshot %}