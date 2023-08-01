{{
    config(
        materialized='table'
    )
}}

select * from {{ ref('fake_stg') }}