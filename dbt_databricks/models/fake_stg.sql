select
1 as id,
current_timestamp() as _timestamp,
2 as dwh_valid_from

union all

select
2 as id,
current_timestamp() as _timestamp,
2 as dwh_valid_from
