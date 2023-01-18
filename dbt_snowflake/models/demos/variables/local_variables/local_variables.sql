
{% set list_of_status = ['success', 'pass', 'fail', 'unknown'] %}
{% set successful_statuses = ['success', 'pass', 'yes', 'successful'] %}


{% for status in list_of_status %}

select
   '{{ status }}' as status,
    {%- if status in successful_statuses %}
      -- is '{{ status }}' a successful status? yes :)
      true
    {%- else %}
      -- is '{{ status }}' a successful status? no :(
      false
    {%- endif %}
    as is_successful
   
   {% if not loop.last %} 
   union all
   {% endif %}
{% endfor %}

