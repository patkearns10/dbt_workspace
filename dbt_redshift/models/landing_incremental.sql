{{
    config(
        materialized='incremental',
        unique_key='profile_id',
        incremental_strategy='merge',
    )
}}

with

{% if is_incremental() %}

incremental_cte as (
    select profile_id
    from {{ ref('landing') }}
    where api_loaded_at > (select max(api_loaded_at) from {{ this }})
    group by 1
),

{% endif %}

original_source as (
    select *
    from {{ ref('landing') }} as src

    {% if is_incremental() %}

    where exists (
        select 1
        from incremental_cte as inc
        where inc.profile_id = src.profile_id
    )

    {% endif %}
)

{% if is_incremental() %}
    , special_columns as (
        -- or could use window function
        select
            profile_id,
            max(external_id) as external_id,
            max(email) as email,
            max(api_loaded_at) as api_loaded_at
        from {{ ref('landing') }} 
        group by profile_id
    )

    select
        original_source.profile_id,
        special_columns.external_id,
        special_columns.email,
        original_source.signup,
        special_columns.api_loaded_at

    from special_columns
    inner join original_source 
    on original_source.profile_id = special_columns.profile_id
    where original_source.email is not null

{% else %}

select * from original_source

{% endif %}