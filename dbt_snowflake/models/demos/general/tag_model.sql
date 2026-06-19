{{
    config(
        tags=["some_tag"]
    )
}}

select 1 as col

---- find all models and tags
{%- for node in graph.nodes.values() -%}
    {%- if node.tags %}
        -- {{ node.name }} has these tags: {{ node.tags }}
    {%- endif -%}
{%- endfor %}

----- for a specific tag
{%- set specific_tag='some_tag' -%}
{%- for node in graph.nodes.values() -%}
    {%- if node.tags -%}
        {%- if specific_tag in node.tags %}
            -- {{ node.name }}
        {%- endif -%}
    {%- endif -%}
{%- endfor %}
