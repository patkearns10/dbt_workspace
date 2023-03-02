{% set relations=dbt_utils.get_relations_by_pattern('customers') %}

{{ dbt_utils.union_relations(relations)}}