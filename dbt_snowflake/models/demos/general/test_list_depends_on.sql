with
orders as  (
    select * from {{ ref('stg_orders' )}}
),
payments as (
    select * from {{ ref('stg_payments') }}
)

select
{{ list_depends_on(graph,this) }} as depends_on_column