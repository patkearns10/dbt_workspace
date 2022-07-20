{% macro v_sql_get_watermark(V_BIW_BATCH_ID) -%}

   {%- call statement('get_edw_watermark', fetch_result=true) %}

        SELECT
            LWM_DTTM,
            HWM_DTTM,
            START_DTTM
        FROM
            -- target db and schema to prevent pulling from prod if run in dev
            {{ target.database }}.{{ target.schema }}.edw_process_batch_ctl as CTL
        WHERE BATCH_ID = {{ V_BIW_BATCH_ID }}
        
    {%- endcall -%}

    {%- set value_list = load_result('get_edw_watermark') -%}
    {%- set default = [] -%}

    {%- if value_list and value_list['data'] -%}
        -- TODO: could probably find a more elegant way to do this
        {%- set V_LWM = value_list['data'][0][0] -%}
        {%- set V_HWM = value_list['data'][0][1] -%}
        {%- set V_START_DTTM = value_list['data'][0][2] -%}

        {{ return([V_LWM, V_HWM, V_START_DTTM]) }}
    {%- else -%}
        {{ return(default) }}
    {%- endif -%}

{%- endmacro %}