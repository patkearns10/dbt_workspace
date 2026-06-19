-- ID 1 deleted, undeleted, deleted
-- ID 2 deleted, undeleted
-- ID 3 no deletes

with snapshot as (
select * from {{ ref('snapshot_delete_and_undelete') }} 
order by id, updated_at desc
)

select 
    *,
    row_number() over (partition by id order by updated_at) as _id_seq,
    lead(dbt_valid_from, 1, null) over (partition by id order by updated_at) as _next_valid_from,
    case
        -- currently active row is not deleted
        when dbt_valid_to is null 
            then 'Active'
        -- middle sequence record valid_to matches next_valid_from means consecutively updated
        when (dbt_valid_to = _next_valid_from) 
            then 'Updated'
        -- last sequenece was to delete the record
        when dbt_valid_to is not null and _next_valid_from is null 
            then 'Deleted'
        -- middle sequence record deleted, but then undeleted
        when (dbt_valid_to != _next_valid_from)
            then 'Deleted and Undeleted'
        else null
    end as status_audit
from snapshot
order by id, _id_seq