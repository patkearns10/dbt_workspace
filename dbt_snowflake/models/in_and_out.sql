-- aka namespaces
-- https://stackoverflow.com/questions/9486393/jinja2-change-the-value-of-a-variable-inside-a-loop


{% set ns = namespace(catalog_name='') %}      
{%- set intermediate_keywords=['int', 'staging', 'fixtures', 'flatfiles', 'dbt'] %}

    {%- if target.name == 'dev' %}
        - {{ ns.catalog_name }} 
        {%- for word in intermediate_keywords %}
            # is '{{ word }}' in '{{this.schema}}'
            {%- if word in this.schema %}
                # yes
                {%- set ns.catalog_name = 'intermediate' %}
                -- {{ ns.catalog_name }}                                  # within if block
                {%- else %}
                # no
                -- {{ ns.catalog_name }}                                  # within if block
            {%- endif %}
        --- {{ ns.catalog_name }}                                         # within for loop
        {%- endfor %}
    {%- else %}
        # else
    {%- endif %}
    --- {{ ns.catalog_name }} 

select '{{ ns.catalog_name }}' as column                              # I would expect this to return 'intermediate'