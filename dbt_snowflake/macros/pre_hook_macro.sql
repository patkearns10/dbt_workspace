{% macro pre_hook_macro() %}
--
-- ========================================== START HOOK SQL
    select 1 as col
-- ========================================== END HOOK SQL
--
{% endmacro %}
