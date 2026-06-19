{{
    config(
        materialized='table'
    )
}}


select * from {{ ref('bar_table') }}
union all
select * from {{ ref('foo_table') }}