{% macro pre_hook_macro() %}

{% set target_relation=adapter.get_relation(this.database, this.schema, this.identifier) %}

-- {{ target_relation }}
--
-- ========================================== START HOOK SQL
    select 1 as col
-- ========================================== END HOOK SQL
--
{% endmacro %}
