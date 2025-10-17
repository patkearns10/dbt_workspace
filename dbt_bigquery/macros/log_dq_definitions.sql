{% macro log_dq_definitions(extract_results) %}
    {%- set test_dict = {} -%}
    {{ log("log_dq_definitions has started running.", info=True) }}
 
    -- parse extract_results and build dictionary with required values for table
    {%- set dq_rule_definitions_table = [] -%}
    {%- set parsed_dq_sum_list = [] %}
    {%- set parsed_dq_def_list = [] %}
    {% for key, value in extract_results.items() %}
        {%- if dq_rule_definitions_table | length == 0 -%}
            {%- do dq_rule_definitions_table.append(value.project ~ '.' ~ value.database_name ~ '.dq_definition') -%}
        {%- endif -%}
        {% for result in value.results %}
            {% set rule_id = result.dp_name ~ '>' ~ value.table_name ~ '>' ~ result.column_name ~ '>' ~ result.dq_dimension ~ '>' ~ result.test_name %}
            {% set list_of_table_columns_list = [] %}
            {%- if result.list_of_table_columns is not none and result.list_of_table_columns | length > 0 -%}
                {% set list_of_table_columns_list = result.list_of_table_columns %}
            {%- endif -%}
            {% set metadata = {
                    'project': value.project,
                    'database_name': value.database_name,
                    'table_name': value.table_name,
                    'column_name': result.column_name,
                    'dq_dimension': result.dq_dimension,
                    'description': result.description,
                    'list_of_table_columns': list_of_table_columns_list
                } %}
            {% set parsed_dq_def_dict = {
                'rule_id': rule_id,
                'business_rule_id': result.business_rule_id,
                'identifier_keys': value.identifier_keys | join(', '),
                'metadata': tojson(metadata)
            }%}
            {% do parsed_dq_def_list.append(parsed_dq_def_dict) %}
        {% endfor %}
    {% endfor %}
{#    {%- do print("parsed_dq_def_list is: " ~ parsed_dq_def_list) %}#}
 
    -- Create table if not exists
    {%- set create_dq_rule_definition_table -%}
        CREATE TABLE IF NOT EXISTS {{ dq_rule_definitions_table[0] }} (
            rule_id STRING,
            business_rule_id STRING,
            identifier_keys STRING,
            metadata JSON
        )
    {%- endset -%}
 
    -- Clear all records in table
    {% set clear_dq_rule_definition_table -%}
        TRUNCATE TABLE {{ dq_rule_definitions_table[0] }}
    {%- endset -%}
 
    -- Insert values into table
    {% set insert_dq_rule_definition -%}
        INSERT {{ dq_rule_definitions_table[0] }} (
            rule_id,
            business_rule_id,
            identifier_keys,
            metadata
        )
        SELECT
            rule_id,
            business_rule_id,
            identifier_keys,
            metadata
        FROM UNNEST([
            STRUCT<rule_id STRING, business_rule_id STRING, identifier_keys STRING, metadata JSON>
            {%- for parsed_dq_def_dict in parsed_dq_def_list -%}
                (
                    '{{ parsed_dq_def_dict.get('rule_id') }}',
                    '{{ parsed_dq_def_dict.get('business_rule_id') }}',
                    '{{ parsed_dq_def_dict.get('identifier_keys') }}',
                    safe.PARSE_JSON('{{ parsed_dq_def_dict.get('metadata') }}')
                ) {{- "," if not loop.last else "" -}}
            {%- endfor -%}
        ])
    {%- endset -%}
 
    -- Execute queries
    {%- do run_query(create_dq_rule_definition_table) -%}
    {%- do run_query(clear_dq_rule_definition_table) -%}
    {%- do run_query(insert_dq_rule_definition) -%}
    {# {%- do apply_grant_for_airflow_wif(dq_rule_definitions_table[0]) -%} #}
 
    {%- do print("log_dq_definitions has finished running") -%}
 
    {{ return ('') }}
{% endmacro %}