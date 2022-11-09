-- this is an unused portion of onsemi/check_sources

{% set sources=get_sources('raw') %}

with

information_schema_relations as (
    select
        table_catalog, -- Database that the table belongs to
        table_schema, --Schema that the table belongs to
        table_name, -- Name of the table
        -- concat_ws('.', table_catalog, table_schema, table_name) as relation_name
        concat_ws('.', table_schema, table_name) as relation_name
    from raw.information_schema.tables 
    -- where table_schema in ('jaffle_shop', 'stripe')
)

select * from information_schema_relations
where 
-- table_schema ilike '{{ target.schema }}%'
-- and
 relation_name in
    (
        {%- for source in sources -%}
            '{{ source.upper() }}'
            {%- if not loop.last -%},{% endif %}
        {%- endfor -%}
    )



