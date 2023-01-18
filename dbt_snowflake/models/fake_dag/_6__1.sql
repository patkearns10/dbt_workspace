select * from {{ ref('_5__1') }}
  union all 
select * from {{ ref('_5__2') }}