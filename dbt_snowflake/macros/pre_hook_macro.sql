{% macro pre_hook_macro() %}

    {% if is_incremental() %}
    -- this will only be applied on an incremental run
    -- 
    -- ========================================== START HOOK SQL
        select 1 as col
    -- ========================================== END HOOK SQL
    --

    {% else %}
    -- 
    -- ========================================== START HOOK SQL
        select 2 as col
    -- ========================================== END HOOK SQL
    --
    {% endif %}
{% endmacro %}
