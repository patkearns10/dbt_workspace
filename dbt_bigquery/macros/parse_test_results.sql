{% macro parse_test_results(results) %}
    {%- do print("parse test results has started running") -%}
 
    {% set parsed_dict = {} %}
    {%- set parsed_results = [] %}
    {%- set max_failed_rows = 50 %}
    {% set generated_uuid_batch_run_id = generate_uuid() %}
 
    -- Every test shall produce a RunResult object, loop through all results and extract relevant data
    {% for run_result in results if run_result.node.resource_type == 'test' %}
        {% set run_result_dict = run_result.to_dict() %}
        {%- do print("individual results: " ~ run_result_dict) %}

        {%- set node = run_result_dict.get('node') -%}
        {%- set failed_rows = [] -%}
        {%- set individual_failed_rows = [] -%}
        {%- set failed_array_length = 0-%}
        {%- set source = run_result_dict.node.relation_name -%}
        {%- set model_test_applied_on = node.get('refs')[0].get('name') -%}

        
        {# ephemeral models #}
        {%- set ephemeral_model_names = [] -%}
        {%- set models = graph.nodes.values() | selectattr('resource_type', 'in', ['model', 'snapshot']) -%}
        {%- for model in models -%}
            {%- if model.config.materialized == 'ephemeral' -%}
                {%- do ephemeral_model_names.append(model.name) -%}
            {%- endif -%}
        {%- endfor -%}

        -- Do not log dq results for ephemeral models
        {% if model_test_applied_on in ephemeral_model_names %}
            {%- do print("ephemeral models: " ~ ephemeral_model_names) %}
            {%- do print("test model name: " ~ model_test_applied_on) %}
            {%- do print("! Skip logging dq results for `" ~ node.get('alias') ~ "` because `" ~ model_test_applied_on ~ "` is materialized as ephemeral") %}
        {% else %}
        
            -- If test was not skipped
            {% if run_result_dict.get('status') != 'skipped' %}
                {% if source is defined and source is not none %}
                    {% set compiled_sql = "select * from " ~ source %}
    
                    {% if run_result_dict.get('status') in ['fail', 'warn'] %}
                        -- This is the table where stored failures are located
                        {% set failed_rows_results = run_query(compiled_sql) %}
                        {% set failed_array_length = run_result_dict.get('failures') %}
    
                        -- Get the failure results from the stored failure (convert data types to string to prevent json serializable errors)
                        {% for row in failed_rows_results.rows[:max_failed_rows] %}
                            {% set row_dict = {} %}
                            {% for k, v in row.dict().items() %}
                                {% do row_dict.update({k: v|string}) %}
                            {% endfor %}
                            {% do failed_rows.append(row_dict) %}
                        {% endfor %}
    
                    {%- endif -%}
                {%- endif -%}
            {%- endif -%}
    
            {% set generated_uuid_rule_sum_id = generate_uuid() %}
    
            -- Extract test column name
            {% set column_name = node.get('column_name') %}
            {%- if column_name == None -%}
                {% set column_name = '_' %}
            {%- endif -%}

            {%- set identifier_keys = node.get('test_metadata').get('kwargs').get('identifier_keys') -%}
            {%- if not identifier_keys -%}

                {%- do print("! No test level `identifier_keys` detected for test `" ~ node.get('alias') ~ "`. Grabbing `indentifier_keys` from the model: `" ~ model_test_applied_on ~ "`.") -%}
                {%- set model_node = graph.nodes.values() | selectattr("name", "equalto", model_test_applied_on) -%}
                    {%- if model_node -%}
                        {%- set node_json = model_node | list | first -%}
                        {# {%- do print("model level identifier_keys: " ~ node_json.get('meta').get('identifier_keys')) %} #}
                        {%- set identifier_keys = node_json.get('meta').get('identifier_keys') -%}
                    {%- endif -%}
            {%- endif -%}
    
            {% set parsed_result_dict = {
                'result_id': invocation_id ~ '.' ~ node.get('unique_id'),
                'invocation_id': invocation_id,
                'unique_id': node.get('unique_id'),
                'test_name': node.get('alias'),
                'resource_type': node.get('resource_type'),
                'status': run_result_dict.get('status'),
                'execution_duration': run_result_dict.get('execution_time'),
                'query': compiled_sql,
                'failed_rows': failed_rows,
                'rule_sum_id': generated_uuid_rule_sum_id,
                'batch_run_id': generated_uuid_batch_run_id,
                'num_rows_failed': failed_array_length,
                'description': (node.get('meta').get('description') or '').replace("'", ""),
                'dq_dimension': node.get('meta').get('dq_dimension'),
                'business_rule_id': (node.get('meta').get('business_rule_id') or '').replace("'", ""),
                'column_name': column_name,
                'list_of_table_columns': node.get('meta').get('list_of_table_columns'),
                'dp_name': node.get('package_name'),
                'identifier_keys': identifier_keys
            } %}
            {% do parsed_results.append(parsed_result_dict) %}
    
            {% set model_name = node.get('file_key_name') | replace("models.", "") %}
    
            -- Append parsed result dict to applicable model in dictionary
            {% if model_name not in parsed_dict %}
                {%- set _ = parsed_dict.update({model_name: {"results": []}}) %}
            {%- endif %}
            {%- do parsed_dict[model_name]["results"].append(parsed_result_dict) %}
        {% endif %}
        
        -- Drop stored failures table
        {% if source is defined and source is not none %}
            {% set drop_sql = "drop table " ~ source %}
            {{ log("$$$dropping table: " ~ source, info=True) }}
            {{ run_query(drop_sql) }}
        {% endif %}
    {% endfor %}
 
    {%- do print("parse test results has finished") -%}

    {{ return(parsed_dict) }}
{% endmacro %}