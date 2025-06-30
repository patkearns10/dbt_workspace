{% macro nesting_dimensions(latest_tables_to_nest, full_tables_to_nest) %}

    with
    
    int_security as (
        select *
        from {{ ref('seed__bcusip') }}  -- #TODO: change to your table name
        qualify row_number() over (partition by bcusip order by asof_date desc) = 1
        
    ),
    
    {%- for table in latest_tables_to_nest %}
    {{ table }} as (
        select *
        from {{ ref(table) }}
    ),
    {%- endfor %}
    
    {%- for table in full_tables_to_nest %}
    {{ table }} as (
        select *
        from {{ ref(table) }}
    ) {{"," if not loop.last}}
    {%- endfor %}
    
    select
        int_security.*,
    
        {%- for table in latest_tables_to_nest %}
        array(
            select as struct
                {{ table }}.* except (bcusip, asof_date)
            from {{ table }}
            where int_security.bcusip = {{ table }}.bcusip
                and int_security.asof_date = {{ table }}.asof_date
        ) as {{ table }},
        {%- endfor %}
    
        {%- for table in full_tables_to_nest %}
        array(
            select as struct
                {{ table }}.*
            from {{ table }}
            where int_security.bcusip = {{ table }}.bcusip
        ) as {{ table }},
        {%- endfor %}
    
    from int_security


{% endmacro %}