
-- Use the `ref` function to select from other models
{{ config(materialized='table') }}


with data as (
    select *
    from {{ ref('my_first_dbt_model') }}

    union all

    select *
    from {{ ref('my_second_dbt_model') }}

    union all

    select *
    from {{ ref('my_third_dbt_model') }}

    union all

    select *
    from {{ ref('my_fourth_dbt_model') }}
)

select *, ROW_NUMBER() OVER (PARTITION BY id order by created_at desc) AS rn
from data

