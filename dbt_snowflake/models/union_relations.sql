{% set relations=dbt_utils.get_relations_by_pattern('dbt_pkearns', 'stg_customers%') %}

{{ dbt_utils.union_relations(relations)}}