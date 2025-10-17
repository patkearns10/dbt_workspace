# auto_decode.sql
{% macro auto_decode(decode_ref, decode_ref_alias, join_conditions=[]) -%}

    {% set sql %}
    with

    decodes as (
        select *
        from {{ ref('seed__decodes') }}
    ),
    
    {{ decode_ref_alias }}_automatically_decoded as (
        select
            {{ decode_ref_alias }}.*,
    
            {%- for join in join_conditions %}
            {%- if join.get('derived', false) %}
            {{ join['new_column'] + "_decode" }}.value as {{ join['new_column'] }},
            {%- else %}
            coalesce({{ join['new_column'] + "_decode" }}.value, {{ join['orig_column'] }}) as {{ join['new_column'] }},
            {%- endif %}
            {%- endfor %}
    
            -- Create an array of all failed decodes for non-derived attributes.
            array(
                select obj from (
                    {%- for join in join_conditions %}
                    {%- if not join.get('derived', false) %} -- Derived decode fails need to be handled separately in the decodefails model.
                    select
                        if({{ join['new_column'] + "_decode" }}.value is null and {{ join['orig_column'] }} is not null,
                            json_object('field', '{{ join['new_column'] }}',
                                    'original_value', {{ join['orig_column'] }},
                                    'tag_name', '{{ join['tag_name'] }}'),
                            null) as obj
                    {%- if not loop.last %} union all {% endif %}
                    {%- endif %}
                    {%- endfor %}
                )
                where obj is not null
            ) as failed_decodes_nonderived
    
        from {{ decode_ref }} as {{ decode_ref_alias }}
    
        {%- for join in join_conditions %}
        left join decodes as {{ join['new_column'] + "_decode" }}
        {%- if join.get('case_insensitive', false) %}
            on lower({{ decode_ref_alias }}.{{ join['orig_column'] }}) = lower({{ join['new_column'] + "_decode" }}.code)
        {%- else %}
            on {{ decode_ref_alias }}.{{ join['orig_column'] }} = {{ join['new_column'] + "_decode" }}.code
        {%- endif %}
            and {{ join['new_column'] + "_decode" }}.tag_name = '{{ join['tag_name'] }}'
        {%- endfor %}
    )
    
    select *
    from {{ decode_ref_alias }}_automatically_decoded


    {% endset %}

    {{ return(sql) }}

{%- endmacro %}