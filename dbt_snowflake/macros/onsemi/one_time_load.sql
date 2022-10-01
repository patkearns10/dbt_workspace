{% macro one_time_load(from) %}
    
    
    {%- set union_relations = dbt_utils.union_relations(relations=[from,ref('seed__negative_key')]) -%}
    {%- set star = dbt_utils.star(from) -%}    

    -- one time load with an added -1 for product key
    union_relations as ({{ union_relations }}),
    one_time_load as (
        select
            -- using this to only select columns from the one_time_load_table, not the extras from other keys
            {{ star }}
        from union_relations
    ),

{% endmacro %}
