{{
    config(
        materialized='table'
    )
}}

select
    id as unique_id,
    _timestamp as created_at,
    dwh_valid_from as foreign_key
from {{ ref('fake_stg') }}
