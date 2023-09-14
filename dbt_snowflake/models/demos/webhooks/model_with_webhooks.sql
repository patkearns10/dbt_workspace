
{% set month_end = 19700101 %}

{{ config(
    post_hook = [
        "{{ log_something_hook() }}",
        "select '" ~ month_end ~ "' as my_date"
        ]
) }}        

select 1 id