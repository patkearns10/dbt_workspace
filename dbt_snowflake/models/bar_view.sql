{{
    config(
        materialized='view'
    )
}}

select * from {{ ref('foo_view') }}