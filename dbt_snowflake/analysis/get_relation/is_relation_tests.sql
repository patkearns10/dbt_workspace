{% set old_etl_relation=adapter.get_relation(
      database=target.database,
      schema="DBT_PKEARNS",
      identifier="CUSTOMERS"
) -%}

-- _is_relation tests

{{ old_etl_relation }}
{{ old_etl_relation.get('metadata', {}) }}
{{ old_etl_relation.get('metadata', {}).get('type', '') }}
{{ old_etl_relation.get('metadata', {}).get('type', '').endswith('Relation') }}
{{ old_etl_relation is mapping }}