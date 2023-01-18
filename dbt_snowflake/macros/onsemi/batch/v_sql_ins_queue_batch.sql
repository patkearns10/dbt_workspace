{% macro v_sql_ins_queue_batch(v_dbt_job_name, ts=current_timestamp) %}
    
  {% set query %}
    CALL UTILITY.EDW_BATCH_QUEUE_PROC('"~{{ v_dbt_job_name }}~"' ,{{ ts }})
  {% endset %}

  {% do run_query(query) %}
{% endmacro %}
