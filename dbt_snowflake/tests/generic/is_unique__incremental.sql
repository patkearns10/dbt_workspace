{% test is_unique__incremental(model, column_name, condition='1=1') %}

{% if execute %}
    -- is_incremental(): {{ is_incremental() }}
    -- flags.FULL_REFRESH: {{ flags.FULL_REFRESH }}
    -- flags.WHICH: {{ flags.WHICH }}
    -- check_model_full_refresh(): {{ check_model_full_refresh() }}
    -- model info : {{ model.unique_id }}
{% endif %}


{% if check_model_full_refresh() %}
    -- this should run on full refresh or first run
    with validation_errors as (
        select
            {{ column_name }}  as unique_field,
            count(*) as n_records

        from {{ model }}
        group by unique_field
        having count(*) > 1
    )

{% else %}
    -- this should run on incremental runs
    with validation_errors as (
        select
            {{ column_name }}  as unique_field,
            count(*) as n_records

        from {{ model }}
        where {{ condition }}
        group by unique_field
        having count(*) > 1
    )
{% endif %}


select *
from validation_errors




{% endtest %}




