{{
    config(
        materialized='table'
    )
}}

-- # Example of what your query should output, materialized as a table
select
'model1' as disable_models

union all

select
'model2' as disable_models
