{% snapshot test_hard_deletes %}

{{
    config(
      target_database=target.database,
      target_schema=target.schema,
      unique_key='id',
      strategy='check',
      check_cols='all'
    )
}}

select 1 as id
union all
select 2 as id
-- union all
-- select 4 as id

{% endsnapshot %}