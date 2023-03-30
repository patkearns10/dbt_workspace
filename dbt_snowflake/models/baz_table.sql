{{
    config(
        materialized='table'
    )
}}


select * from {{ ref('bar_table') }}