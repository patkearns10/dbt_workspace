{% test unique_coded_exceptions(model, column_name) %}

{# get filter clause exceptions from seed__coded_test_exceptions #}
{%- set get_column_values_filter %}
    model_name = '{{ model.identifier }}' and column_name = '{{ column_name }}'
{%- endset %}

{%- set where_filters = dbt_utils.get_column_values(
    table=ref('seed__coded_test_exceptions'),
    column='filter_clause',
    where=get_column_values_filter,
    default=["1=1"]
) -%}

select
    {{ column_name }} as unique_field,
    count(*) as n_records
from {{ model }}
where {{ column_name }} is not null

-- add exceptions filters from csv
--------------------------------
{% for where_filter in where_filters -%}
    and {{ where_filter }}
{% endfor -%}
--------------------------------
group by {{ column_name }}
having count(*) > 1

{% endtest %}