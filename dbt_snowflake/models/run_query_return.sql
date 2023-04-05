{% set payment_methods_query -%}
    select status, payment_method from {{ ref('stg_payments') }}
    order by 1
{% endset -%}

{% set customers_query -%}
    select distinct first_name from {{ ref('stg_customers') }}
    order by 1
{% endset -%}

{% set payment_results = run_query(payment_methods_query) -%}
{% set customer_results  = run_query(customers_query) -%}

{%- if execute -%}
    {# Return the first column #}
        {% set payments_results_list = payment_results.columns[0].values() | list -%}
-----------------------------------
-----------------------------------
{% set ns = namespace(column_chars=0, row_chars=0, total_chars=0) -%}
{% for column in payment_results.column_names -%}
{%- set ns.column_chars = ns.column_chars + (column | length) -%}
{% endfor %}
--- columns = {{ payment_results.columns | length }}
--- column_chars = {{ ns.column_chars }}

{% for item in range(payment_results.columns | length) -%}
    {% for word in payment_results.columns[item].values() | list -%}
            {%- set ns.row_chars = ns.row_chars + (word | length) -%}
    {% endfor -%}
{% endfor %}
--- rows = {{ payment_results.rows | length }}
--- row_chars = {{ ns.row_chars }}
--- total_chars = {{ ns.column_chars + ns.row_chars }}

        {% set customers_results_list = customer_results.columns[0].values() | list -%}

    {%- else -%}
        {% set payments_results_list = [] -%}
        {% set customers_results_list = [] -%}
{%- endif %}

select {{ payments_results_list }} as results
union all
select {{ customers_results_list }} as results
