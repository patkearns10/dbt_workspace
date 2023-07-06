
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    select 1 as id,
    '2000-01-01' as created_at
    union all
    select 2 as id,
    '2000-01-01' as created_at
    union all
    select 3 as id,
    '2000-01-01' as created_at
    union all
    select 4 as id,
    '2000-01-01' as created_at
    union all
    select 5 as id,
    '2000-01-01' as created_at
    union all
    select 6 as id,
    '2000-01-01' as created_at
    union all
    select 7 as id,
    '2000-01-01' as created_at
    union all
    select 8 as id,
    '2000-01-01' as created_at
    union all
    select 9 as id,
    '2000-01-01' as created_at
    union all
    select 10 as id,
    '2000-01-01' as created_at
    union all
    select 11 as id,
    '2000-01-01' as created_at
    union all
    select 12 as id,
    '2000-01-01' as created_at
    union all
    select null as id,
    '2000-01-01' as created_at

union all

    select 1 as id,
    '1000-01-01' as created_at
    union all
    select 2 as id,
    '1000-01-01' as created_at
    union all
    select 3 as id,
    '3000-01-01' as created_at
    union all
    select 4 as id,
    '3000-01-01' as created_at
    union all
    select 5 as id,
    '1000-01-01' as created_at
    union all
    select 6 as id,
    '1000-01-01' as created_at
    union all
    select 7 as id,
    '1000-01-01' as created_at
    union all
    select 8 as id,
    '3000-01-01' as created_at
    union all
    select 9 as id,
    '3000-01-01' as created_at
    union all
    select 10 as id,
    '3000-01-01' as created_at
    union all
    select 11 as id,
    '3000-01-01' as created_at
    union all
    select 12 as id,
    '3000-01-01' as created_at
    union all
    select null as id,
    '1000-01-01' as created_at



)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
