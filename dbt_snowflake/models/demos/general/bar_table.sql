{{
    config(
        materialized='table'
    )
}}


select * from {{ ref('foo_table') }}