select * from {{ ref('_3__1') }}
  union all 
select * from {{ ref('_3__2') }}