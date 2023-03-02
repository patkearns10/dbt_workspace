{%- set catalog_name='default' %}
{%- set intermediate_keywords=['int', 'staging', 'fixtures', 'flatfiles', 'dbt'] %}
{%- if target.name == 'dev' %}
    - {{ catalog_name }} 
    {%- for word in intermediate_keywords %}
        # is '{{ word }}' in '{{this.schema}}'
        {%- if word in this.schema %}
            # yes
            {%- set catalog_name = 'intermediate' %}
            -- {{ catalog_name }}                                  # within if block
            {%- else %}
            # no
            -- {{ catalog_name }}                                  # within if block
        {%- endif %}
    --- {{ catalog_name }}                                         # within for loop
    {%- endfor %}
{%- else %}
    # else
{%- endif %}

select '{{ catalog_name }}' as column                              # I would expect this to return 'intermediate'