{% set old_etl_relation=adapter.get_relation(
      database=target.database,
      schema="DBT_PKEARNS",
      identifier="CUSTOMERS"
) -%}

{{ dbt_utils.get_filtered_columns_in_relation(from=old_etl_relation) }}
