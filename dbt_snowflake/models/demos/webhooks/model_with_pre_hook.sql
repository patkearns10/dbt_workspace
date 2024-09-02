{{
    config(
        pre_hook = "{{ pre_hook_macro() }}",
        snowflake_warehouse='AD_HOC'
    )
}}

-- ========================================== START MODEL SQL
select 2 as col
-- ========================================== END MODEL SQL