{% macro get_relation_test() %}
        
    {% set source_relation = adapter.get_relation(
        database=target.database,
        schema=target.schema,
        identifier="sample_data") -%}
    {{ log("Source Relation: " ~ source_relation, info=true) }}

    {% set dbt_relation=ref('sample_data') %}
    {{ log("dbt Relation: " ~ dbt_relation, info=true) }}

{% endmacro %}