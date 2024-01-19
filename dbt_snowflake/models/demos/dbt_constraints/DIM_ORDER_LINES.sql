select
1 as OL_PK,
10 as OL_UK,
100 as OL_CUSTKEY

union all

select
2 as OL_PK,
20 as OL_UK,
200 as OL_CUSTKEY

union all

select
3 as OL_PK,
30 as OL_UK,
300 as OL_CUSTKEY

union all

select
4 as OL_PK,
40 as OL_UK,
400 as OL_CUSTKEY

-- more changes
-- include: {{ ref('customers') }}
-- NEED TO TRY WITH SNAPSHOTS