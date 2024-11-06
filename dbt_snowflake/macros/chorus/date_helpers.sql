
/*
select
--timestamp is always default_as_per_project_yml "etl_at_timestamp_default" or what is passed as "etl_at_timestamp_provided"
 {{ get_etl_at_timestamp()}} no_modify
,{{ get_etl_at_timestamp(-1)}} minus_one_day
,{{ get_etl_at_timestamp(2,'day')}} plus_two_days
,{{ get_etl_at_timestamp(-3,'month',true)}} minus_three_months_but_trunc -- to month first
*/

{%- macro get_etl_at_timestamp(offset_value=0,date_or_time_part='None',trunc_to_part=false) -%}
  {#- for models requiring an etl_at_ts for constraining snapshots pass a variable in ISO timestamp format e.g. --vars '{"etl_at_timestamp_provided":"2024-02-02"}' -#}
  {#- if variable etl_at_ts_provided is not passed etl_at_ts_default will be used, evaluates based on project.yml e.g. dateadd(day,0,current_timestamp()::timestamp_ntz) -#}
  {#- modify in models by passing an offset follows the same convention as timestamp_add#}
  {#- trunc_to_part value of true will add a date_trunc wrapper to the timestamp truncing to to the date_or_time_part #}
      
    {# checks if the macro is being executed in a real dbt run or just being compiled for syntax validation. If it is not being executed (not execute), it returns an empty string.#}
    {# why doesnt this work#}
    {# if not execute %} {{ return("") }}{% endif #}

    {% if date_or_time_part == 'None'%}
        {%- set date_or_time_part = 'day' -%}
    {% endif %}

     {% if trunc_to_part %}
        {%- set date_trunc_start_txt = 'date_trunc(' + date_or_time_part +',' -%}
        {%- set date_trunc_end_txt = ')' -%}
      {%else%}  
        {%- set date_trunc_start_txt = '' -%}
        {%- set date_trunc_end_txt = '' -%}
    {% endif %}

    {# Use your date var provided, or pickup no_runtime_date_var for default handling#}
    {% set timestamp =  var("etl_at_timestamp_provided","no_runtime_date_var")  %}
    
    {% if timestamp == "no_runtime_date_var" %}
        {# return the project default #}
        {% set timestamp = var('etl_at_timestamp_default') %}
    {% else %}  
        {# use provide value which will be text and needs quoting and cast #}
        {% set timestamp = "'" + timestamp + "'::timestamp_ntz" %}          
    {% endif %}
    
    {%if offset_value == 0 %}
        {# nothing to do #}
        {% set get_etl_at_timestamp = timestamp   %}
    {% else %}
        {# add the modifiers#}
        {% set get_etl_at_timestamp = "dateadd(" + date_or_time_part + "," + offset_value|string + "," + date_trunc_start_txt + var('etl_at_timestamp_default') + date_trunc_end_txt + ")" %}
    {% endif%}

    {#{{ log("Logging get_etl_at_timestamp : " ~  get_etl_at_timestamp  ) }}#}
    {{ return(get_etl_at_timestamp) }}
        
{%- endmacro -%}

{%- macro get_firstload_reset_backto_timestamp(offset_value=0,date_or_time_part='None',trunc_to_part=false) -%}
  {#- for models requiring an etl_at_ts for constraining snapshots pass a variable in ISO timestamp format e.g. --vars '{"etl_at_timestamp_provided":"2024-02-02"}' -#}
  {#- if variable get_firstload_reset_backto_timestamp_provided is not passed get_firstload_reset_backto_timestamp_default will be used, evaluates based on project.yml e.g. dateadd(day,0,current_timestamp()::timestamp_ntz) -#}
  {#- modify in models by passing an offset follows the same convention as timestamp_add#}
  {#- trunc_to_part value of true will add a date_trunc wrapper to the timestamp truncing to to the date_or_time_part #}
      
    {# checks if the macro is being executed in a real dbt run or just being compiled for syntax validation. If it is not being executed (not execute), it returns an empty string.#}
    {# why doesnt this work#}
    {# if not execute %} {{ return("") }}{% endif #}

    {% if date_or_time_part == 'None'%}
        {%- set date_or_time_part = 'day' -%}
    {% endif %}

     {% if trunc_to_part %}
        {%- set date_trunc_start_txt = 'date_trunc(' + date_or_time_part +',' -%}
        {%- set date_trunc_end_txt = ')' -%}
      {%else%}  
        {%- set date_trunc_start_txt = '' -%}
        {%- set date_trunc_end_txt = '' -%}
    {% endif %}

    {# Use your date var provided, or pickup no_runtime_date_var for default handling#}
    {% set timestamp =  var("firstload_reset_backto_timestamp_provided","no_runtime_date_var")  %}
    
    {% if timestamp == "no_runtime_date_var" %}
        {# return the project default #}
        {% set timestamp = var('firstload_reset_backto_timestamp_default') %}
    {% else %}  
        {# use provide value which will be text and needs quoting and cast #}
        {% set timestamp = "'" + timestamp + "'::timestamp_ntz" %}          
    {% endif %}
    
    {%if offset_value == 0 %}
        {# nothing to do #}
        {% set get_firstload_reset_backto_timestamp = timestamp   %}
    {% else %}
        {# add the modifiers#}
        {% set get_firstload_reset_backto_timestamp = "dateadd(" + date_or_time_part + "," + offset_value|string + "," + date_trunc_start_txt + var('etl_at_timestamp_default') + date_trunc_end_txt + ")" %}
    {% endif%}

    {#{{ log("Logging get_firstload_reset_backto_timestamp : " ~  get_etl_at_timestamp  ) }}#}
    {{ return(get_firstload_reset_backto_timestamp) }}
        
{%- endmacro -%}
