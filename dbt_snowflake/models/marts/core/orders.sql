with orders as  (

    select * from {{ ref('stg_orders' )}}

),

payments as (

    select * from {{ ref('fct_orders') }}
    
),

order_payments as (

    select

        order_id,
        order_cost as amount

    from payments
),

final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        coalesce(order_payments.amount, 0) as amount
    from orders
    left join order_payments using (order_id)
)

select * from final