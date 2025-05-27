{% snapshot snapshot_batch_file %}

{{
    config(
      target_database='development',
      target_schema='snapshots',
      unique_key='id',
      strategy='check',
      check_cols='all'
    )
}}


{% set relation_exists = load_relation(this) is not none %}

select * from {{ ref('seed__batch_file') }}
where file_date in (
    {% if relation_exists %}
        select dateadd(day, 1, max(file_date)) from {{ this }}
    {% else %}
        select min(file_date) from {{ ref('seed__batch_file') }}
    {% endif %}
    )

{% endsnapshot %}