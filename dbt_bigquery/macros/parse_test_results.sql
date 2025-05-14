{% macro parse_test_results(results) %}
    {%- do print("parse_test_results has started running with the following run results: " ~ results) %}


    {% set parsed_dict = {} %}
    {%- set parsed_results = [] %}
    {%- set max_failed_rows = 50 %}
    
    {# {% set generated_uuid_batch_run_id = generate_uuid() %} #}

    -- Every test shall product a RunResult object, loop through all results and extract relevent data
    {%- do print("results complete is: " ~ results) %}
    {% for run_result in results %}
        {% set run_result_dict = run_result.to_dict() %}
        {%- do print("individual results: " ~ run_result_dict) %}
        {% set node = run_result_dict.get('node') %}
        {% if node.get('resource_type') == 'test' %}
 
            {%- set failed_rows = [] -%}
            {%- set individual_failed_rows = [] -%}
            {% set failed_array_length = 0 %}
            -- if test was not skipped
            {% if run_result_dict.get('status') != 'skipped'%}
                {{ log("$$$run_result_dict: " ~ run_result_dict | tojson(indent=4) ) }}
                 {% if run_result_dict.get('status') == 'fail' %}
                    -- this is the table where store failures are stored
                    {% set compiled_sql = "select * from " ~  run_result_dict.node.relation_name %}
                    {% set failed_rows_results = run_query(compiled_sql) %}
                    {% set failed_array_length = run_result_dict.get('failures') %}
                    
                    -- get the failure results from the stored failure
                    {% for row in failed_rows_results.rows[:max_failed_rows] %}
                      {% do failed_rows.append(row.dict()) %}
                    {% endfor %}
                    
                    -- drop store failures table
                    {% set drop_sql = "drop table " ~  run_result_dict.node.relation_name %}
                    {{ log("$$$dropping table: " ~  run_result_dict.node.relation_name) }}
                    {{ run_query(drop_sql) }}

                {%- endif -%}
            {%- endif -%}
 
            {# {% set generated_uuid_rule_sum_id = generate_uuid() %} #}
 
            -- Extract test column name
            {% set column_name = node.get('column_name') %}
            {%- if column_name == None -%}
                {% set column_name = '_' %}
            {%- endif -%}
 
            {% set parsed_result_dict = {
                    'result_id': invocation_id ~ '.' ~ node.get('unique_id'),
                    'invocation_id': invocation_id,
                    'unique_id': node.get('unique_id'),
                    'test_name': node.get('name'),
                    'resource_type': node.get('resource_type'),
                    'status': run_result_dict.get('status'),
                    'execution_duration': run_result_dict.get('execution_time'),
                    'query': compiled_sql,
                    'failed_rows': failed_rows,
                    'rule_sum_id': 123,
                    'batch_run_id': 456,
                    'num_rows_failed': failed_array_length,
                    'description': node.get('meta').get('description'),
                    'dq_dimension': node.get('meta').get('dq_dimension'),
                    'business_rule_id': node.get('meta').get('business_rule_id'),
                    'column_name': column_name
                    }%}
            {% do parsed_results.append(parsed_result_dict) %}
            {% set model_name = node.get('file_key_name') | replace("models.", "") %}
 
            -- append parsed result dict to applicable model in dictionary   
            {% if model_name not in parsed_dict %}
                {%- set _ = parsed_dict.update({model_name: []}) %}
            {%- endif %}
            {%- do parsed_dict[model_name].append(parsed_result_dict) %}
 
        {%- endif -%}

 
    {% endfor %}
 
    {{ log("parsed_results for all RunResults is: " ~ parsed_results) }}
    {{ log("parsed_dict returned for all RunResults is: " ~ parsed_dict) }}
    {{ return(parsed_dict) }}
{% endmacro %}