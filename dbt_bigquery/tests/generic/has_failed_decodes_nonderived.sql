{% test has_failed_decodes_nonderived(model) -%}
-- depends_on: {{ ref('seed__decodes') }}  --- # TODO: change this to your decodes model
--- # TODO: take the join_conditions and use a variable perhaps? Or like all possible combinations.

{%- set join_conditions = [
    {
        "orig_column": "option",
        "new_column": "option_decode",
        "tag_name": "OPT_TYPES",
        "derived": false
    },
    {
        "orig_column": "paymentclass",
        "new_column": "paymentclass_decoded",
        "tag_name": "PAYMENT",
        "derived": false
    }
] %}

----

    with

    decodes as (
        select *
        from {{ ref('seed__decodes') }}   --- # TODO: change this to your decodes model
    ),
    
    automatically_decoded as (
        select
            model_to_decode.*,
    
            -- Create an array of all failed decodes for non-derived attributes.
            array(
                select obj from (
                    {%- for join in join_conditions %}
                    select
                            json_object('field', '{{ join['new_column'] }}',
                                    'original_value', {{ join['orig_column'] }},
                                    'tag_name', '{{ join['tag_name'] }}'
                                    )
                                    as obj
                    {%- if not loop.last %} union all {% endif %}
                    {%- endfor %}
                    )
                ) as failed_decodes_nonderived
    
        from {{ model }} as model_to_decode
    
        {%- for join in join_conditions if not join.get('derived', false) %} -- Derived decode fails need to be handled separately in the decodefails model.
            left join decodes as {{ join['new_column'] + "_decode" }}
            {%- if join.get('case_insensitive', false) %}
                on lower(model_to_decode.{{ join['orig_column'] }}) = lower({{ join['new_column'] + "_decode" }}.code)
            {%- else %}
                on model_to_decode.{{ join['orig_column'] }} = {{ join['new_column'] + "_decode" }}.code
            {%- endif %}
                and {{ join['new_column'] + "_decode" }}.tag_name = '{{ join['tag_name'] }}'
        {% endfor %}
        
        where
        {%- for join in join_conditions if not join.get('derived', false) %} 
            ({{ join['new_column'] + "_decode" }}.value is null and {{ join['orig_column'] }} is not null)
            {% if not loop.last %}or{% endif %}
        {%- endfor %}
    )
    
    select *
    from automatically_decoded

{%- endtest %}