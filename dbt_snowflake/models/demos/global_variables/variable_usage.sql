{{
    config(
        materialized='table',
        pre_hook="{{ variable_update() }}"
    )
}}

{% set local_var="'my_quoted_var'" %}

select 
    '{{ var('my_cool_var') }}'::date as col_a,
    {{ local_var }} as col_b,
    {{ var('disable_models') }} as var_disable_models,
    '{{ disable_models }}' as disable_models,
    
    {%- if "some_model_name" in var('disable_models') %}
        true 
        {%- else %}
        false
    {%- endif %} 
    as some_flag,
    '{{ this.name }}' as this__name,
    {%- if this.name in dbt_utils.get_column_values(ref('disable_list_create'), 'disable_models') %} 
        true 
        {%- else %}
        false
    {%- endif %} 
    as some_other_flag,

{% set disable_models="['some_other_model_name']" %}

    {{ disable_models }} as disable_models_updated
