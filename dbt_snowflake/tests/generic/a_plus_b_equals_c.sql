{% test a_plus_b_equals_c(model, model_a, model_b) %}

{#-- Needs to be set at parse time, before we return '' below --#}
{{ config(fail_calc = 'sum(coalesce(diff_count, 0))') }}

{#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
{%- if not execute -%}
    {{ return('') }}
{% endif %}

with a as (
    select
        1 as id,
        '{{ model_a }}' as model_a,
        count(*) as count_a
    from {{ model_a }}
),

b as (
    select
        1 as id,
        '{{ model_b }}' as model_b,
        count(*) as count_b
    from {{ model_b }}
),

c as (
    select
        1 as id,
        '{{ model }}' as model_c,
        count(*) as count_c
        from {{ model }}
),

final as (

    select
        model_a,
        count_a,
        model_b,
        count_b,
        model_c,
        count_c,
        abs(count_c - count_a - count_b) as diff_count
    from a
    full join b using (id)
    full join c using (id)
)

select * from final

{% endtest %}