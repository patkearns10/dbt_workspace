{{
    config(
        materialized='ephemeral',
    )
}}

with

cte_1 as (
    select 1 as some_column
)

select * from cte_1
