-- find dupes
select {{ dbt_utils.star(ref('dedupe_incremental')) }}
from 
    (
    select *, row_number() over (partition by id order by insert_time desc) as _row_number
    from {{ ref('dedupe_incremental') }}
    )
where _row_number != 1
