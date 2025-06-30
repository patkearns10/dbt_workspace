{%- set latest_tables_to_nest = [
    'seed__putcalls',
    'seed__convertibles',
] %}
 
{%- set full_tables_to_nest = [
    'seed__security_history',
] %}


with final as (
{{ nesting_dimensions(latest_tables_to_nest, full_tables_to_nest) }}
order by bcusip
)

select * from final