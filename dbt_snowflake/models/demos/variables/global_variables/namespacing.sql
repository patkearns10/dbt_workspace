{% set ns = namespace(foo='bar') %}
{% set foo = 'baz' %}

select 
'{{ ns.foo }}' as namespaced_var,
'{{ foo }}' as regular_var
