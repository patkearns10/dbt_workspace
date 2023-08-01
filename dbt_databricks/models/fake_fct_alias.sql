{{
    config(
        materialized='table'
    )
}}

select
    id as unique_id,
    id as other_id
from {{ ref('fake_stg') }}
