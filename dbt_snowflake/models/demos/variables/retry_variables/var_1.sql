
-- {{ var('do_something') }}
select 1 as col

{% if var('do_something') == 'yes' %}
    union all 
    select 1 as col
{% endif %}