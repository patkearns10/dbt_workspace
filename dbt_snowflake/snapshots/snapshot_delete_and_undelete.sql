{% snapshot snapshot_delete_and_undelete %}

{{
    config(
      target_database='development',
      target_schema='snapshots',
      unique_key='id',
      strategy='check',
      check_cols='all',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

select
 3 as id,
 'updated' as status,
 current_timestamp() as updated_at

union all

select
 2 as id,
 'undeleted' as status,
 current_timestamp() as updated_at


{% endsnapshot %}