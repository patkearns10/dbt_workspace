{% macro parse_test_results(results) %}
    {%- do print("parse test results has started running") -%}
 
    {% set parsed_dict = {} %}
    {%- set parsed_results = [] %}
    {%- set max_failed_rows = 50 %}
    {% set generated_uuid_batch_run_id = generate_uuid() %}
 
    -- Every test shall produce a RunResult object, loop through all results and extract relevant data
    {% for run_result in results if run_result.node.resource_type == 'test' %}
        {% set run_result_dict = run_result.to_dict() %}
{#        {%- do print("individual results: " ~ run_result_dict) %}#}
        {% set node = run_result_dict.get('node') %}
 
        {%- set failed_rows = [] -%}
        {%- set individual_failed_rows = [] -%}
        {% set failed_array_length = 0 %}
        {% set source = run_result_dict.node.relation_name %}
 
        -- If test was not skipped
        {% if run_result_dict.get('status') != 'skipped' %}
            {% if source is defined and source is not none %}
                {% set compiled_sql = "select * from " ~ source %}
 
                {% if run_result_dict.get('status') in ['fail', 'warn'] %}
                    -- This is the table where stored failures are located
                    {% set failed_rows_results = run_query(compiled_sql) %}
                    {% set failed_array_length = run_result_dict.get('failures') %}
 
                    -- Get the failure results from the stored failure
                    {% for row in failed_rows_results.rows[:max_failed_rows] %}
                        {% do failed_rows.append(row.dict()) %}
                    {% endfor %}
 
                {%- endif -%}
            {%- endif -%}
        {%- endif -%}
 
        -- Drop stored failures table
        {% if source is defined and source is not none %}
            {% set drop_sql = "drop table " ~ source %}
            {{ log("$$$dropping table: " ~ source, info=True) }}
            {{ run_query(drop_sql) }}
        {% endif %}
 
        {% set generated_uuid_rule_sum_id = generate_uuid() %}
 
        -- Extract test column name
        {% set column_name = node.get('column_name') %}
        {%- if column_name == None -%}
            {% set column_name = '_' %}
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
            'dp_name': node.get('package_name')
        } %}
        {% do parsed_results.append(parsed_result_dict) %}
 
        {% set model_name = node.get('file_key_name') | replace("models.", "") %}
 
        -- Append parsed result dict to applicable model in dictionary
        {% if model_name not in parsed_dict %}
            {%- set _ = parsed_dict.update({model_name: {"results": []}}) %}
        {%- endif %}
        {%- do parsed_dict[model_name]["results"].append(parsed_result_dict) %}
    {% endfor %}
 
    {%- do print("parse test results has finished") -%}
    {{ return(parsed_dict) }}
{% endmacro %}