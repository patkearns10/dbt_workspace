{{
    config(
        materialized='view',
        meta = {
            'single_key': 'override'
        }
    )
}}

{{ log_something() }}

select * from {{ ref('foo_view') }}
