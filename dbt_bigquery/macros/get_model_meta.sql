{% macro get_model_meta(model) %}
    {% set model_str = model | string %}
    {% set clean_relation = model_str | replace("`", "") %}

    {# Extract just the table name (last part after dot) #}
    {% set table_name = clean_relation.split('.')[-1] %}

    -- clean_relation: {{ clean_relation }}
    -- table_name: {{ table_name }}

    {% set node = graph.nodes.values() | selectattr("name", "equalto", table_name) %}
    {% if node %}
        {% set node_json = node | list | first %}
        {{ return(node_json.get('meta', {})) }}
    {% endif %}
{% endmacro %}