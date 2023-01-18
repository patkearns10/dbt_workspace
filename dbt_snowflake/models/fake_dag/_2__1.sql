select * from {{ ref('_1__1') }}
  union all 
select * from {{ ref('_1__2') }}