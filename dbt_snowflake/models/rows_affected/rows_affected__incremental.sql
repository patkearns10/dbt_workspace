{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

select
    1 as id
union all
select
    2 as id
union all
select
    3 as id
union all
select
    4 as id
union all
select
    5 as id
union all
select
    6 as id
union all
select
    7 as id

{% if is_incremental() %}
union all
select
    8 as id
{% endif %}