with datas as (
    select * from {{ ref('unit_test_sample_data') }}
),

inc as (
    select
        *,
        case 
            when
                order_type != 'L'
                and limit_value is not null
            then null
            else 1
        end as include_exclude
    from datas
)

select * from inc