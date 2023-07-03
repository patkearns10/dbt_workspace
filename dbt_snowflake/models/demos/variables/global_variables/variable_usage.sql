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

    {{ disable_models }} as disable_models_updated,

    -- non existent variable (variable not defined)
    '{{ var("non_existent_variable", "default_value") }}' as non_existent_variable,
    -- variable defined in dbt_project.yml as '2016-01-01'
    '{{ var("my_cool_var", "default_value") }}' as existent_variable,
    -- define non existent variable (variable defined in sql)
    {% set my_cool_var='2021-01-01' %} 
    '{{ my_cool_var }}' as now_existent_variable,
    -- global var not the same as variable defined in sql
    '{{ var("my_cool_var") }}' as var_variable,
    -- switch from var to set variable
    {% set my_cool_var_mirror=var("my_cool_var") %}
    '{{ my_cool_var_mirror }}' as mirror_variable,
    -- now you can alter the value
    {% set my_cool_var_mirror="2022-01-01" %}
    '{{ my_cool_var_mirror }}' as changed_variable,

    -- non existent variable (variable not defined) overwritten by env_var
    '{{ var("non_existent_variable", env_var("DBT_WAREHOUSE")) }}' as overwrite_in_command
