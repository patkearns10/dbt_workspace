with

security_decoded as (
    select * from {{ ref('int_security_decoded')}}
),

case_when as (
    select
        bcusip,
        coalesce(
            (
                select floater_record.INDX2
                from unnest(security_decoded.FLOATER_set) as floater
                where change_date >= floater.FLOATER_record.START_DT and change_date < floater.FLOATER_record.END_DT
                order by floater.FLOATER_record.START_DT asc -- double check this ordering in the python
                limit 1
            ),
            if (
                change_date < FLOATER_set[safe_offset(0)].FLOATER_record.START_DT,
                FLOATER_set[safe_offset(0)].FLOATER_record.INDX2,
                null
            ),
            if (
                change_date >= FLOATER_set[safe_offset(array_length(FLOATER_set) - 1)].FLOATER_record.START_DT,
                FLOATER_set[safe_offset(array_length(FLOATER_set) - 1)].FLOATER_record.INDX2,
                null
            )
        ) as index2
    from security_decoded
)

select * from case_when