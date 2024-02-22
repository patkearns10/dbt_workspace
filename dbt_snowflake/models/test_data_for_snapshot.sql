{{
    config(
        materialized='table',
            snowflake_warehouse='AD_HOC'
    )
}}



-- sample data

select
 1 as id,
 '1999-01-02' as updated_at,
 'y' as column_to_exclude

 union all
 
 select
 2 as id,
 '2024-01-03' as updated_at,
 'y' as column_to_exclude

 union all
 
 select
 3 as id,
 '2023-01-03' as updated_at,
 'y' as column_to_exclude