{% snapshot snapshot_update %}

{{
    config(
      target_database='development',
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at'
    )
}}

-- sample data
select
 1 as id,
 '2023-01-04' as updated_at,
 'y' as new_col

 union all
 
 select
 2 as id,
 '2023-01-04' as updated_at,
 'y' as new_col

 union all
 
 select
 3 as id,
 '2023-01-04' as updated_at,
 'y' as new_col

{% endsnapshot %}