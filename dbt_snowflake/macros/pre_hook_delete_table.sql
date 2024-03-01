{% macro delete_table_condition(sourceobject,condition) %}
    {%- set source_relation = adapter.get_relation(
      database=sourceobject.database,
      schema=sourceobject.schema,
      identifier=sourceobject.name) -%}
    {% set table_exists=source_relation is not none %}
    {% set query %}
        -- DELETE FROM {{this}} WHERE {{condition}}
        select 1 as col ;
    {% endset %}

    {% if table_exists %}
        {{ log("Table exists, performing delete", info=True) }}
        {{ query }}
    {% endif %}
{% endmacro %}