{{
    config(
        materialized='table',
        enabled=false
    )
}}


with data as (
    select * from {{ ref('customer_orders_dummy_join') }}
),

datas as (
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data
union
select row_number() over (partition by customer_id order by customer_name, first_order_date) as ro, * from data

)

select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas
union
select distinct * from datas

