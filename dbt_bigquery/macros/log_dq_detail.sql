{% macro log_dq_detail(extract_results) %}
    {%- set test_dict = {} -%}
    {%- do print("log_dq_detail has started running") -%}

    -- parse extract_results and build dictionary with required values for table
    {%- set dq_rule_details_table = [] -%}
    {%- set parsed_dq_sum_list = [] %}
    {%- set parsed_dq_detail_list = [] %}
    {% set parsed_dq_detail_dict = {} %}
   
    {% for key, value in extract_results.items() %}
 
        {%- if dq_rule_details_table | length == 0 -%}
            {%- do dq_rule_details_table.append(value.project ~ '.' ~ value.database_name ~ '.dq_detail') -%}
        {%- endif -%}
        {% for result in value.results %}
            {% set rule_id = value.database_name ~ '.' ~ value.table_name ~ '.' ~ result.column_name ~ '.' ~ result.test_name %}
            {% set result_detail_id = result.invocation_id ~ '.' ~ result.unique_id %}
            {% for error in result.failed_rows %}
                -- convert data types to string to prevent json serializable errors
                {% set safe_error = {} %}
                {% for k, v in error.items() %}
                    {% do safe_error.update({k: v|string}) %}
                {% endfor %}
                {% set parsed_dq_detail_dict = {
                    'result_detail_id': result_detail_id,
                    'rule_sum_id': result.rule_sum_id,
                    'rule_id': rule_id,
                    'error_record': tojson(safe_error)
                } %}
                {% do parsed_dq_detail_list.append(parsed_dq_detail_dict) %}
            {% endfor %}
        {% endfor %}
    {% endfor %}
{#    {%- do print("parsed_dq_detail_list is: " ~ parsed_dq_detail_list) %}#}
 
    -- Create table if not exists
    {% set create_dq_rule_detail_table -%}
        CREATE TABLE IF NOT EXISTS {{ dq_rule_details_table[0] }} (
            result_detail_id STRING,
            rule_sum_id STRING,
            rule_id STRING,
            error_record JSON
        )
    {%- endset -%}
 
     -- Insert values into table
    {% set insert_dq_rule_detail -%}
        INSERT {{ dq_rule_details_table[0] }} (
            result_detail_id,
            rule_sum_id,
            rule_id,
            error_record
        )
        SELECT
            result_detail_id,
            rule_sum_id,
            rule_id,
            error_record
        FROM UNNEST([
            STRUCT<result_detail_id STRING, rule_sum_id STRING, rule_id STRING, error_record JSON>
            {%- for parsed_dq_detail_dict in parsed_dq_detail_list -%}
                (
                    GENERATE_UUID(),
                    '{{ parsed_dq_detail_dict.get('rule_sum_id') }}',
                    '{{ parsed_dq_detail_dict.get('rule_id') }}',
                    safe.PARSE_JSON('{{ parsed_dq_detail_dict.get('error_record') }}')
                ) {{- "," if not loop.last else "" -}}
            {%- endfor -%}
        ])
    {%- endset -%}
 
    -- Execute queries
    {%- do run_query(create_dq_rule_detail_table) -%}
    {% if parsed_dq_detail_list | length > 0 %}
        {%- do run_query(insert_dq_rule_detail) -%}
    {% endif %}
    {# {%- do apply_grant_for_airflow_wif(dq_rule_details_table[0]) -%} #}
 
    {%- do print("log_dq_detail has finished running") -%}
 
    {{ return ('') }}
{% endmacro %}