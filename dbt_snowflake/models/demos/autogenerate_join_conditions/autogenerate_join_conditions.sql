{%- set qualified_fields=['orders.order_id', 'orders.customer_id'] -%}
{%- set joins=['orders', 'stg_customers'] -%}

with
customers as (select * from {{ ref('customers') }}),
orders as (select * from {{ ref('orders') }}),
stg_customers as (select * from {{ ref('stg_customers') }})

select
    customers.lifetime_value,

    {%- for qualified_field in qualified_fields %}
        {%- set field=qualified_field.split('.')[1] %}
        coalesce( {{qualified_field}} , -1) as {{ field }},
    {%- endfor %}

    customers.number_of_orders
from customers
    {%- for join in joins %}
        left outer join {{ join }}
        on customers.customer_id = {{ join }}.customer_id
    {%- endfor %}






