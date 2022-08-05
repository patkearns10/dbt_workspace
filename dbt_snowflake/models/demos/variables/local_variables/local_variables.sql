
{% set list_of_status = ['success', 'pass', 'fail', 'unknown'] %}
{% set successful_statuses = ['success', 'pass', 'yes', 'successful'] %}

{#
select
{% for status in list_of_status %}
    {%- if status in successful_statuses %}
       -- is '{{ status }}' a successful status? yes :)
    {%- else %}
       -- is '{{ status }}' a successful status? no :(
    {%- endif %}
{% endfor %}
#}