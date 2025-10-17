{% macro log_dq_summary(extract_results) %}
    {%- set test_dict = {} -%}
    {%- do print("log_dq_summary has started running") -%}
 
    -- parse extract_results and build dictionary with required values for table
    {%- set dq_rule_summary_table = [] -%}
    {%- set parsed_dq_sum_list = [] %}
{#    {%- do print("extract results for summary is: " ~ extract_results) %}#}
    {% for key, value in extract_results.items() %}
        {%- if dq_rule_summary_table | length == 0 -%}
            {%- do dq_rule_summary_table.append(value.project ~ '.' ~ value.database_name ~ '.dq_summary') -%}
        {%- endif -%}
        {% for result in value.results %}
            {% set rule_id = result.dp_name ~ '>' ~ value.table_name ~ '>' ~ result.column_name ~ '>' ~ result.dq_dimension ~ '>' ~ result.test_name %}
            {% set parsed_dq_sum_dict = {
                'rule_sum_id': result.rule_sum_id,
                'rule_id': rule_id,
                'batch_run_id': result.batch_run_id,
                'total_table_rows': value.total_table_rows,
                'num_rows_failed': result.num_rows_failed,
                'compiled_sql': result.query,
                'execution_time': run_started_at.strftime("%Y-%m-%d %H:%M:%S"),
                'execution_duration': result.execution_duration,
                'run_result': result.status
            }%}
            {% do parsed_dq_sum_list.append(parsed_dq_sum_dict) %}
        {% endfor %}
    {% endfor %}
{#    {%- do print("parsed_dq_sum_list is: " ~ parsed_dq_sum_list) %}#}
 
    -- Create table if not exists
    {% set create_dq_rule_summary_table -%}
        CREATE TABLE IF NOT EXISTS {{ dq_rule_summary_table[0] }} (
            rule_sum_id STRING,
            rule_id STRING,
            batch_run_id STRING,
            total_table_rows INT,
            num_rows_failed INT,
            compiled_sql STRING,
            execution_time STRING,
            execution_duration STRING,
            run_result STRING
        )
    {%- endset -%}
 
    -- Insert values into table
    {% set insert_dq_rule_summary -%}
        INSERT {{ dq_rule_summary_table[0] }} (
            rule_sum_id,
            rule_id,
            batch_run_id,
            total_table_rows,
            num_rows_failed,
            compiled_sql,
            execution_time,
            execution_duration,
            run_result
        )
        SELECT
            rule_sum_id,
            rule_id,
            batch_run_id,
            total_table_rows,
            num_rows_failed,
            compiled_sql,
            execution_time,
            execution_duration,
            run_result
        FROM UNNEST([
            STRUCT<rule_sum_id STRING, rule_id STRING, batch_run_id STRING, total_table_rows INT, num_rows_failed INT, compiled_sql STRING, execution_time STRING, execution_duration STRING, run_result STRING>
            {%- for parsed_dq_sum_dict in parsed_dq_sum_list -%}
                (
                    '{{ parsed_dq_sum_dict.get('rule_sum_id') }}',
                    '{{ parsed_dq_sum_dict.get('rule_id') }}',
                    '{{ parsed_dq_sum_dict.get('batch_run_id') }}',
                    {{ parsed_dq_sum_dict.get('total_table_rows') | int }},
                    {{ parsed_dq_sum_dict.get('num_rows_failed') | int }},
                    '{{ parsed_dq_sum_dict.get('compiled_sql') }}',
                    '{{ parsed_dq_sum_dict.get('execution_time') }}',
                    '{{ parsed_dq_sum_dict.get('execution_duration') }}',
                    '{{ parsed_dq_sum_dict.get('run_result') }}'
                ) {{- "," if not loop.last else "" -}}
            {%- endfor -%}
        ])
    {%- endset -%}
 
    -- Execute queries
    {%- do run_query(create_dq_rule_summary_table) -%}
    {%- do run_query(insert_dq_rule_summary) -%}
    {# {%- do apply_grant_for_airflow_wif(dq_rule_summary_table[0]) -%} #}
 
    {%- do print("log_dq_summary has finished running") -%}
 
    {{ return ('') }}
{% endmacro %}