{{
    config(
        materialized='incremental',
        unique_key='scd2_pk'
    )
}}


with
new_data_plus_latest_previous_value as (
    select * from {{ ref('upstream_id_changes') }}
    {% if is_incremental() %}
        where unique_id in (
            select distinct unique_id
            from {{ ref('upstream_id_changes') }}
            where _date >= (select max(this._date) from {{ this }} as this)
        )
        and _date >= (
        select max(_date)
        from {{ this }}
        where is_current

    )

    {% endif %}
),

add_scd2_logic as (
    select
        {{ dbt_utils.generate_surrogate_key(['unique_id', '_date']) }} as scd2_pk,
        unique_id as upstream_pk,
        _date,
        color,
        _date as valid_from,
        lead(_date) over (partition by unique_id order by _date) as valid_to,
        (valid_to is null) as is_current
    from
        new_data_plus_latest_previous_value
)

select * from add_scd2_logic
order by _date