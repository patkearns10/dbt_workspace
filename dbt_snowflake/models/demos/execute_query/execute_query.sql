{% set your_query %}
-- this should return 100
select count(customer_id) from {{ ref('customers') }} where customer_id is not null
{% endset %}

{% set results = run_query(your_query) %}


{% if execute %}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

select
{% for value in results_list %}
'{{ value }}' as col,
    {% if value == 100 %}
        'is equal to 100'
    {% else %}
        'is NOT equal to 100'
    {% endif %}
    as some_context_col
{% endfor %}