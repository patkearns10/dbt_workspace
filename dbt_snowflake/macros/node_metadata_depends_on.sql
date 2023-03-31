{% macro list_depends_on(graph=graph, this=this) %}
    {% if execute %}
        {%- if this.name == 'request' -%}
            ['this does not work with Preview button']
        {%- else -%}
            {% for node in graph.nodes.values() -%}
                {%- if node.name == this.name -%}
                    {{ node.depends_on.nodes}}
                {%- endif -%}
            {%- endfor %}
        {%- endif -%}
    {%- else -%}
        [that]
    {%- endif -%}
{% endmacro %}