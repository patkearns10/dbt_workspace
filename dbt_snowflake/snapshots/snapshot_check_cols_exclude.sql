{% snapshot snapshot_check_cols_exclude %}
-- development.snapshots.snapshot_check_cols_exclude
{{
    config(
      target_database='development',
      target_schema='snapshots',
      unique_key = "id",
      strategy='check',
      check_cols = ["_check_columns_sk"],
      invalidate_hard_deletes = true
    )
}}

{% set column_names = get_columns_in_relation( ref('test_data_for_snapshot'))|map(attribute='name')|reject("in", ['COLUMN_TO_EXCLUDE'])|list %}
-- {{  column_names }}

select
    *,
    {{ dbt_utils.generate_surrogate_key(column_names) }} as _check_columns_sk
from {{ ref('test_data_for_snapshot') }}

{% endsnapshot %}