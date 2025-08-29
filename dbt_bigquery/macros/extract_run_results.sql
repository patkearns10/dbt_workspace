{% macro extract_run_results(results) %}
    {% do print("extract_run_result has started running") %}
 
    -- Loop through run results and extract and store results into dictionary
    {%- set parsed_results_dict = parse_test_results(results) -%}
 
    -- Loop through all nodes in manifest.json and extract all relevent metadata from models
    {% if execute %}
        {% for node in graph.nodes.values() %}
            {% if node.name in parsed_results_dict and node.resource_type == "model" and node.config.materialized != 'ephemeral' %}
                {% set total_table_rows = get_row_count(node.database, node.schema, node.name, node.config.materialized) %}
                {% set _ = parsed_results_dict[node.name].update({'identifier_keys': node.meta.identifier_keys}) %}
                {% set _ = parsed_results_dict[node.name].update({'project': node.database}) %}
                {% set _ = parsed_results_dict[node.name].update({'database_name': node.schema}) %}
                {% set _ = parsed_results_dict[node.name].update({'table_name': node.name}) %}
                {% set _ = parsed_results_dict[node.name].update({'total_table_rows': total_table_rows}) %}
            {% endif %}
        {% endfor %}
    {% endif %}
 
{#    {%- do print("parsed_results_dict after is: " ~ parsed_results_dict) %}#}
    {% do print("extract_run_result has finished running") %}
    {{ return(parsed_results_dict) }}
{% endmacro %}