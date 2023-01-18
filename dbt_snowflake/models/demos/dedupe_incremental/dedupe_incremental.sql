{{ config(materialized='table') }}

select
    1 as id,
    'green' as color,
    42 as the_answer,
    current_timestamp() as insert_time

union all

select
    2 as id,
    'purple' as color,
    42 as the_answer,
    current_timestamp() as insert_time

union all

select
    1 as id,
    'green' as color,
    42 as the_answer,
    current_timestamp() as insert_time