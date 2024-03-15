{{
    config(
        materialized='view',
        meta = {
            'single_key': 'override'
        }
    )
}}

{{ log_something() }}
-- comment

select * from {{ ref('foo_view') }}
