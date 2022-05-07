-- # your incremental block
{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

-- # if block at the top to see if this model should be run, pulls from your query
-- # this.name checks the name of this model and compares to the values returned for the the models that should be run
{%- if this.name in dbt_utils.get_column_values(ref('disable_list_create'), 'disable_models') and is_incremental() %}

    -- # skip this model
    select * from {{ this }} limit 0
    
{%- else %}
    
    -- # run this model
    -- TODO: replace this with your usual incremental SQL code here
    select
        'example data' as some_col

{%- endif %}
