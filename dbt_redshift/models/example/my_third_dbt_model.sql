
-- Use the `ref` function to select from other models

{{ config(materialized='table') }}

select *
from {{ ref('my_second_dbt_model') }}
where id = 1
