{{
    config(
        materialized='materialized_view'
    )
}}

select * from {{ ref('sample_data') }}