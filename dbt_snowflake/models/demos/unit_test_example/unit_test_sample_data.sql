with datas as (
    select
        1        as id,
        'L'      as order_type,
        10       as limit_value
    union all
    select
        2        as id,
        'L'      as order_type,
        null:int as limit_value
    union all
    select
        3        as id,
        'M'      as order_type,
        null:int as limit_value
    union all
    select
        4       as id,
        'M'     as order_type,
        20::int as limit_value
)

select * from datas