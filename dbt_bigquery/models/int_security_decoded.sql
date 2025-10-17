with

datas as (
    select
        'ABC' as bcusip,
        '2021-08-13' as change_date,
        [
            struct(
                struct(
                    '2021-08-01' as START_DT,
                    '2021-08-10' as END_DT,
                    1.81 as INDX2
                ) as FLOATER_record
            ),
            struct(
                struct(
                    '2021-08-12' as START_DT,
                    '2023-08-14' as END_DT,
                    2.32 as INDX2
                ) as FLOATER_RECORD
            )
        ] as floater_set
    
    union all
    
        select
        'DEF' as bcusip,
        '2022-09-16' as change_date,
        [
            struct(
                struct(
                    '2020-01-01' as START_DT,
                    '2020-02-01' as END_DT,
                    5.32 as INDX2
                ) as FLOATER_record
            ),
            struct(
                struct(
                    '2020-02-02' as START_DT,
                    '2022-09-16' as END_DT,
                    4.44 as INDX2
                ) as FLOATER_RECORD
            )
        ] as floater_set
)

select * from datas