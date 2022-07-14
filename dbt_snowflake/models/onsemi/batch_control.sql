{{
    config(
        post_hook="{{ v_sql_upd_success(v_dbt_job_name) }}"
    )
}}


-- TODO: consider changing job name to the name of the file so you could use something standard like {{ this.name }} or some version of that (like fully qualified name)
    -- I am unsure how to get the on-run-end hook to update failures otherwise
{%- set v_dbt_job_name = 'DBT_MART_SALES_BILLING_FACT'-%}
    --suggested alternative:
    -- {%- set v_dbt_job_name = this.name -%}  or {%- set v_dbt_job_name = this -%}

-- Step 1 Create new batch ID
{{ v_sql_ins_queue_batch(v_dbt_job_name, current_timestamp) }}

-- Step 2 Set the batch to running
{{ v_sql_upd_run_batch(v_dbt_job_name) }}

-- Step 3 Fetch the batch id
{%- set V_BIW_BATCH_ID = v_sql_get_batch(v_dbt_job_name) -%}

-- Step 4 Get the High and Low water mark
{%- set V_LWM = v_sql_get_watermark(V_BIW_BATCH_ID)[0] -%}
{%- set V_HWM = v_sql_get_watermark(V_BIW_BATCH_ID)[1] -%}
{%- set V_START_DTTM = v_sql_get_watermark(V_BIW_BATCH_ID)[2] -%}


select
{{ V_BIW_BATCH_ID }}  as batch_id,
{{ V_LWM }} as V_LWM,
{{ V_HWM }} as V_HWM,
{{ V_START_DTTM }} as V_START_DTTM

--------
-- ... rest of the SQL here
--------

-- Step 5 Success or Failure to ETL table
-- success happens in post-hook, which runs only if the model succeeds
-- failure happes via on-run-end hook: https://gist.github.com/jeremyyeo/064106e480106b49cd337f33a765ef20