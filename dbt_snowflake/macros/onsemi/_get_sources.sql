{% macro get_sources(database_name=target.database) %}

  {#
    Get all sources in your dbt project that are from `database_name`

    Example input:
    ---------------------------------------------------------------
    {{ get_sources('raw') }}

    Example output
    ---------------------------------------------------------------
    ['raw.jaffle_shop.customers', 'raw.jaffle_shop.orders', 'raw.stripe.payment']
  #}
  
  {% set sources = [] -%}
  {% if execute %}
    {% for node in graph.sources.values() -%}
      {%- if node.database == database_name -%}
            {# 
          {%- do sources.append(node.relation_name) -%}
            -- "relation_name":"snowflake.account_usage.warehouse_load_history",
            -- "database":"snowflake",
            -- "schema":"account_usage",
            -- "name":"warehouse_load_history",
            -- "source_name":"snowflake_meta",
            #}
          {%- do sources.append(node.schema ~ '.' ~ node.name) -%}
      {%- endif -%}
    {%- endfor %}
  {% endif %}
    {{ return(sources) }}
{% endmacro %}