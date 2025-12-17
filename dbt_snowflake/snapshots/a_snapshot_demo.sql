{% snapshot a_snapshot_demo %}

{{
    config(
      target_database='development',
      target_schema='dbt_pkearns',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
      hard_deletes='invalidate'
    )
}}

-- sample data
select
    1 as id,
    '2025-01-01 00:00:00.000' as updated_at,
    'x' as new_col

union all

select
    2 as id,
    '2025-01-01 00:00:00.000' as updated_at,
    'x' as new_col

union all

select
    4 as id,
    '2025-12-17 01:15:02.021' as updated_at,
    'y' as new_col

{% endsnapshot %}