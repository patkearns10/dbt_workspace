# auto_decode.sql
{% macro auto_decode_v2(decode_ref, decode_ref_alias, join_conditions=[]) -%}

    {% set sql %}

        with

        decodes as (
            select *
            from {{ ref('seed__decodes') }}   --- # TODO: change this to your decodes model
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

        
            from {{ decode_ref }} as {{ decode_ref_alias }}
        
            {%- for join in join_conditions if not join.get('derived', false) %} -- Derived decode fails need to be handled separately in the decodefails model.
                left join decodes as {{ join['new_column'] + "_decode" }}
                {%- if join.get('case_insensitive', false) %}
                    on lower({{ decode_ref_alias }}.{{ join['orig_column'] }}) = lower({{ join['new_column'] + "_decode" }}.code)
                {%- else %}
                    on {{ decode_ref_alias }}.{{ join['orig_column'] }} = {{ join['new_column'] + "_decode" }}.code
                {%- endif %}
                    and {{ join['new_column'] + "_decode" }}.tag_name = '{{ join['tag_name'] }}'
            {% endfor %}

            where
            {%- for join in join_conditions if not join.get('derived', false) %} 
                (
                ({{ join['new_column'] + "_decode" }}.value is null and {{ join['orig_column'] }} is null)
                or
                ({{ join['new_column'] + "_decode" }}.value is not null and {{ join['orig_column'] }} is not null)
                )
                {% if not loop.last %}and{% endif %}
            {%- endfor %}
        )
        
        select *
        from {{ decode_ref_alias }}_automatically_decoded

    {% endset %}

    {{ return(sql) }}

{%- endmacro %}

