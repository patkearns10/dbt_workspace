{{ config(materialized='table') }}

select
    1 as id,
    'green' as color,
    42 as the_answer,
    current_timestamp() as insert_time

union all

select
    3 as id,
    'purple' as color,
    42 as the_answer,
    current_timestamp() as insert_time

union all

select
    5 as id,
    'red' as color,
    42 as the_answer,
    current_timestamp() as insert_time