{% test not_null_sampling(model, column_name) %}

{% set column_list = '*' if should_store_failures() else column_name %}

with sampled_data as (
    select {{ column_list }}
    from {{ model }} SAMPLE (10)
)

select * from sampled_data
where {{ column_name }} is null

{% endtest %}