{{
    config(
        materialized='incremental',
        unique_key='unique_id'
    )
}}

{#
default values:
    is_backfill: false
    refresh_start_date: '1970-01-01'
    refresh_end_date: '2999-01-01'

command to run:
    `dbt run -s incremental_steps_override --vars '{is_backfill: true, refresh_start_date: '2024-01-01', refresh_end_date: '2024-01-03'}'`
#}

select 
    *,
    current_timestamp as _updated_at
from {{ ref('upstream_example') }}
where 1=1

{% if target.name == 'ci' %}
-- only run in slim CI
   and _date >= (current_date - interval '1 day')
{% endif %}

{% if var('is_backfill') %}
-- only run in adhoc backfill
  and _date between '{{ var('refresh_start_date') }}' and '{{ var('refresh_end_date') }}'
{% endif %}

{% if is_incremental() and not var('is_backfill') %}
-- only run if incremental load
  and _date >= (select max(_date) from {{ this }})
{% endif %}
