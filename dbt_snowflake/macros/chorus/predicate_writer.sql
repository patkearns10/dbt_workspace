{% macro predicate_writer(rel=ref('upstream_example'), join_key='unique_id') %}

{% set sql %}
["exists (select 1 from {{ rel }} s where DBT_INTERNAL_DEST.{{join_key}} = s.{{join_key}})", "select 1 as col"]
{% endset %}

{{ sql }}

{% endmacro %}