{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        event_time='ordered_at',
        batch_size='day',
        lookback=3,
        begin='2016-09-01'
    )
}}

select * from {{ ref('stg_orders') }}