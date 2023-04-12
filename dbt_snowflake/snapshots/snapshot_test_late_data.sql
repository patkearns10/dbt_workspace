{% snapshot snapshot_test_late_data %}

{{
    config(
      target_database='development',
      target_schema='snapshots',
      unique_key='id',

      strategy='timestamp',
      updated_at='updated_at',
    )
}}

-- sample data
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