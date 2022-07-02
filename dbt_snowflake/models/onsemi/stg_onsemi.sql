with

sample_data as (
    select
        1 as CORPORATION_CODE,
        1 as BIW_INS_DTTM,
        1 as BIW_UPD_DTTM,
        1 as BIW_BATCH_ID,
        11 as generic_id,
        11 as unique_id,
        'Pass' as sometimes_bad_id,
        current_timestamp as _updated_at

    union all

    select
        2 as CORPORATION_CODE,
        2 as BIW_INS_DTTM,
        2 as BIW_UPD_DTTM,
        2 as BIW_BATCH_ID,
        22 as generic_id,
        22 as unique_id,
        'Pass' as sometimes_bad_id,
        current_timestamp as _updated_at

    union all

    select
        3 as CORPORATION_CODE,
        3 as BIW_INS_DTTM,
        3 as BIW_UPD_DTTM,
        3 as BIW_BATCH_ID,
        33 as generic_id,
        33 as unique_id,
        'Fail' as sometimes_bad_id,
        current_timestamp as _updated_at
)

select
    *
from sample_data
