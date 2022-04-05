with

testers as (
    
  select 1 as user_id, 1 as org_id
    union all
  select null as user_id, 2 as org_id
    union all
  select 3 as user_id, null as org_id
    union all
  select null as user_id, null as org_id

),

testers_2 as (
    
  select 1 as user_id, 1 as org_id
    union all
  select null as user_id, 2 as org_id
    union all
  select 3 as user_id, null as org_id
    union all
  select null as user_id, null as org_id

)

select * from testers
union all
select * from testers_2

-- natural full outer join testers_2
-- where user_id is not null or org_id is not null
