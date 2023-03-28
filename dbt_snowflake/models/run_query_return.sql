{% set payment_methods_query -%}
    select distinct payment_method from {{ ref('stg_payments') }}
    order by 1
{% endset -%}

{% set customers_query -%}
    select distinct first_name from {{ ref('stg_customers') }}
    order by 1
{% endset -%}

{% set payment_results = run_query(payment_methods_query) -%}
    -- {{ payment_results | length }}
{% set customer_results  = run_query(customers_query) -%}
    -- {{ customer_results | length }}

{%- if execute -%}
    {# Return the first column #}
        {% set payments_results_list = payment_results.columns[0].values() | list -%}
        {% set customers_results_list = customer_results.columns[0].values() | list -%}
    {%- else -%}
        {% set payments_results_list = [] -%}
        {% set customers_results_list = [] -%}
{%- endif %}

select {{ payments_results_list }} as results
union all
select {{ customers_results_list }} as results
