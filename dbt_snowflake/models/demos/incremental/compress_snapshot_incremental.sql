{{
    config(
        materialized='incremental',
        unique_key=['id', 'name', 'cve__status__c', 'status_iteration']
    )
}}

-- so the problem here is that things can go forward to next status, then back to previous status.
-- need to do an extra step to figure out if that happens create counts for steps in the order they go in. 

with with_change_flag as (
    select
        *,
        -- get previous status
        lag(cve__status__c) over (partition by id, name order by dbt_valid_from) as prev_status,
        -- did the status change?
        case
            when cve__status__c != prev_status
            then 1
            else 0
        end as is_new_status,
        -- current record flag
        case
            when dbt_valid_to is null
            then 1
            else 0
        end as is_current,

    from {{ ref('seed_claims') }}
    
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        -- you cannot simply bring in records that happened since the last run (the delta) because
        -- we need to do counts and reconcilliation across a whole id/name so we need to bring in previous records
            
        where id in (
            select id from {{ref('seed_claims') }}
            where dbt_updated_at > (
                select max(dbt_updated_at) from {{ this }}
                )
        )
    {% endif %}
),

with_grouping as (
    select *,
        -- increment by 1 when we switch to a different status (this is necessary because status changes can go backwards and forwards)
        sum(is_new_status) over (partition by id, name order by dbt_valid_from rows unbounded preceding) as status_iteration
    from with_change_flag
),

compress_records as (
    select
        id,
        name,
        cve__status__c,
        status_iteration,
        max(dbt_updated_at) over (partition by id, name, cve__status__c, status_iteration order by dbt_updated_at) as dbt_updated_at,
        min(dbt_valid_from) over (partition by id, name, cve__status__c, status_iteration order by dbt_valid_from) as dbt_valid_from,
        -- group by doesn't work because it is not returning the null value for current record.
        -- if you can figure that out, we could remove these window functions in favor of group by
        case
            when is_current = 1 then null
            else max(dbt_valid_to) over (partition by id, name, cve__status__c, status_iteration order by dbt_valid_to)
        end as dbt_valid_to,
        row_number() over (partition by id, name, cve__status__c, status_iteration order by dbt_valid_to desc) as rn
    from with_grouping
    -- thus we need to remove other rows other than the most recent of each status_iteration
    qualify rn = 1
),

get_latest as (
    select
        id,
        name,
        cve__status__c,
        status_iteration,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from
        compress_records
),

ordered as (
    -- you can remove this CTE, just using it to make the data easier to read
    select * from get_latest
    order by dbt_valid_from desc
)

select * from ordered