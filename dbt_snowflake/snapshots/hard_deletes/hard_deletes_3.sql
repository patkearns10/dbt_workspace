{% snapshot hard_deletes_3 %}

{{
    config(
      target_database='development',
      target_schema='snapshots',
      unique_key='id',
      check_cols='all',
      strategy='check',
      invalidate_hard_deletes=True,
    )
}}

select
1 as id,
2 as other_id


{% endsnapshot %}