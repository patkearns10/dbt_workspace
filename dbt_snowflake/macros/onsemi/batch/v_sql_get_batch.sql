{% macro v_sql_get_batch(v_dbt_job_name) -%}

    {%- set query -%}
        SELECT
            NVL(MAX(BATCH_ID),-1001) as V_BIW_BATCH_ID
        FROM
            -- target db and schema to prevent pulling from prod if run in dev
            {{ target.database }}.{{ target.schema }}.edw_process_batch_ctl as CTL
            INNER JOIN
            {{ target.database }}.{{ target.schema }}.edw_process_info as INFO
            ON CTL.PROCESS_ID = INFO.PROCESS_ID     
            AND INFO.PROCESS_NAME= '{{ v_dbt_job_name }}'
            AND CTL.BATCH_STATUS='R' 
    {%- endset -%}

    {# -- Prevent querying of db in parsing mode #}
    {%- if execute -%}
        {%- set results = run_query(query) -%}
        {%- set output = results.columns[0].values()[0] -%}
    {%- endif -%}

    {{ output }}

{%- endmacro %}
