{%- set target_sources=var('source_database') -%}
{%- set target_database=var('target_database') -%}
{%- set sources=get_sources(target_sources) -%}

-- sources exist in this database:
  -- {{ target_sources }}

-- check if sources exist in this database:
  -- {{ target_database }}

-- list of sources to check:
  -- {{ sources }}

select * from (
  {%- for source in sources %}
    select count(1) from {{ target_database }}.{{ source }} {% if not loop.last %} union all {% endif %}
  {%- endfor %}
)
