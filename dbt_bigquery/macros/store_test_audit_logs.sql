/*
  --add "{{ store_test_audit_logs(results) }}" to an on-run-end: block in dbt_project.yml 
*/

{% macro store_test_audit_logs(results) %}
  {%- set test_results = [] -%}

  {%- for result in results if result.node.resource_type == 'test' -%}
    {%- set test_results = test_results.append(result) -%}
  {%- endfor -%}

  {% if test_results|length == 0 -%}
    {{ log("store_test_results found no test results to process.") if execute }}
    {{ return('') }}
  {% endif -%}

  {%- set central_tbl -%} {{ target.schema }}.test_audit_log_central {%- endset -%}
  {%- set history_tbl -%} {{ target.schema }}.test_audit_log_history {%- endset -%}
  
  {{ log("Test logging notes")}}
  {{ log("Centralizing " ~ test_results|length ~ " test results in " + central_tbl, info = true) if execute }}
  {{ log(test_results, info=true) }}
  create or replace table {{ central_tbl }} as (
  
  {%- for result in test_results %}

    {%- set test_name = '' -%}
    {%- set test_type = '' -%}
    {%- set column_name = '' -%}

    {%- if result.node.test_metadata is defined -%}
      {%- set test_name = result.node.test_metadata.name -%}
      {%- set test_type = 'generic' -%}
      
      {%- if test_name == 'relationships' -%}
        {%- set column_name = result.node.test_metadata.kwargs.field ~ ',' ~ result.node.test_metadata.kwargs.column_name -%}
      {%- else -%}
        {%- set column_name = result.node.test_metadata.kwargs.column_name -%}
      {%- endif -%}
    {%- elif result.node.name is defined -%}
      {%- set test_name = result.node.name -%}
      {%- set test_type = 'singular' -%}
    {%- endif %}
    
    select
        cast('{{ test_name }}' as string)  as test_name,
        cast('{{ result.node.unique_id }}' as string)  as model_name,
        cast('{{ result.node.config.severity }}' as string)  as test_severity_config,
        cast('{{ result.status }}' as string)  as test_result,
        cast('{{ process_refs(result.node.refs) }}' as string)  as model_refs,
        cast('{{ process_refs(result.node.sources, is_src=true) }}' as string)  as source_refs,
        cast('{{ column_name|escape }}' as string)  as column_names,
        cast('{{ result.node.name }}' as string)  as test_name_long,
        cast('{{ test_type }}' as string)  as test_type,
        cast('''{{ result.node.compiled_code }}''' as string)  as compiled_code,
        to_json_string(x) as test_failures_json,
        cast('{{ result.execution_time }}' as string)  as execution_time_seconds,
        cast('{{ result.node.original_file_path }}' as string)  as file_test_defined,
        cast('{{ var("pipeline_name", "variable_not_set") }}' as string)  as pipeline_name,
        cast('{{ var("pipeline_type", "variable_not_set") }}' as string)  as pipeline_type,
        cast('{{ target.name }}' as string)  as dbt_cloud_target_name,
        cast('{{ env_var("DBT_CLOUD_PROJECT_ID", "manual") }}' as string)  as _audit_project_id,
        cast('{{ env_var("DBT_CLOUD_JOB_ID", "manual") }}' as string)  as _audit_job_id,
        cast('{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as string)  as _audit_run_id,
        cast('{{ env_var("DBT_CLOUD_URL", "https://cloud.getdbt.com/develop/12906/projects/232737") }}' || ''||'{{ env_var("DBT_CLOUD_PROJECT_ID", "manual") }}'||'/runs/'||'{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}'  as string )  as _audit_run_url,
        current_timestamp as _timestamp
    from (
        select * from {{ result.node.relation_name }}
        ) as x
    {{ "union all" if not loop.last }}
  
  {%- endfor %}
  
  );
  {% if target.name != 'default' %}
      create table if not exists {{ history_tbl }} as (
        select 
          {{ dbt_utils.generate_surrogate_key(["test_name", "test_result", "_timestamp"]) }} as sk_id, 
          * 
        from {{ central_tbl }}
        where false
        -- what is the purpose of ^this false condition? is it tied to the jinja logic?
      );
    insert into {{ history_tbl }} 
      select 
       {{ dbt_utils.generate_surrogate_key(["test_name", "test_result", "_timestamp"]) }} as sk_id, 
       * 
      from {{ central_tbl }}
    ;
  {% endif %}
{% endmacro %}
/*
  return a comma delimited string of the models or sources were related to the test.
    e.g. dim_customers,fct_orders
  behaviour changes slightly with the is_src flag because:
    - models come through as [['model'], ['model_b']]
    - srcs come through as [['source','table'], ['source_b','table_b']]
*/
{% macro process_refs( ref_list, is_src=false ) %}
  {% set refs = [] %}
  {% if ref_list is defined and ref_list|length > 0 %}
      {% for ref in ref_list %}
        {% if is_src %}
          {{ refs.append(ref|join('.')) }}
        {% else %}
          {{ refs.append(ref['name']) }}
        {% endif %} 
      {% endfor %}
      {{ return(refs|join(',')) }}
  {% else %}
      {{ return('') }}
  {% endif %}
{% endmacro %} 
