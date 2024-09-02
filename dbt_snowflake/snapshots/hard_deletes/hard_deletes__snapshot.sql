{% snapshot hard_deletes__snapshot %}

{{
    config(
      target_database='development',
      target_schema='snapshots',
      unique_key='id',
      check_cols=['color'],
      strategy='check',
      invalidate_hard_deletes=True,
    )
}}

select * from {{ ref('hard_deletes_source') }}

{% endsnapshot %}