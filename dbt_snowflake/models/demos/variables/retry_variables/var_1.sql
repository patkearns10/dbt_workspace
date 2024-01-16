
-- {{ var('do_something') }}
select 1 as col
    
    union all 
    
{% if var('do_something') == 'yessir' %}
    select 2 as col
{% endif %}