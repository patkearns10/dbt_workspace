{% macro v_sql_upd_run_batch(v_dbt_job_name) %}
    
  {% set query %}
    CALL UTILITY.EDW_BATCH_RUNNING_PROC('"~{{ v_dbt_job_name }}~"')
  {% endset %}

  {% do run_query(query) %}
{% endmacro %}
