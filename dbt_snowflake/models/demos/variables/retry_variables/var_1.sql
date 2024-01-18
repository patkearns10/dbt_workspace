
-- {{ var('do_something') }}
select 1 as col
    
{% if var('do_something') == 'yessir' %}
    union all 
    
    select 2 as col
{% endif %}