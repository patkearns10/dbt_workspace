with 

ephemeral_1 as (
    select * from {{ ref('ephemeral_1') }}
)

select * from ephemeral_1
