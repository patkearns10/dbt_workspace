{% set old_etl_relation=adapter.get_relation(
      database=target.database,
      schema="DBT_PKEARNS",
      identifier="CUSTOMERS"
) -%}

{{ dbt_utils._is_relation(obj=old_etl_relation) }}