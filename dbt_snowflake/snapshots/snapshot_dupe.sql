{% snapshot snapshot_dupe %}

{{
    config(
      target_database='development',
      target_schema='snapshots',
      unique_key='some_value',
      strategy='check',
      check_cols=['id', 'some_value'],
    )
}}

-- sample data
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