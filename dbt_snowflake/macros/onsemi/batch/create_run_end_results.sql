{% macro create_run_end_results_table() %}

  {{ log('Creating `dbt_meta__run_end_results` table if not exists.', info=True) }}

  create table if not exists {{ target.database }}.{{ target.schema }}.dbt_meta__run_end_results (
      run_invocation_id text not null,
      model_identifier text not null,
      model_name text not null,
      relation_name text not null,
      materialization text not null,
      model_status text not null,
      model_message text not null,
      updated_at timestamp not null
  );

{% endmacro %}