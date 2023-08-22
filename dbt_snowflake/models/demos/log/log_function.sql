{{
    config(
        post_hook="{{ log('===================================Finishing run for model: ' ~ this.table, info=True) }}"
    )
}}

{{ log("===================================Starting to run model: " ~ this.table, info=True) }}

select 1 as some_column
