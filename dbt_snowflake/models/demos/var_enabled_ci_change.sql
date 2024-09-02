{% if var('do_something') == 'true' %}
    select 'did something' as col
{% else%}
    select 'nope' as col
{% endif %}
