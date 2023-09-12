select * from {{ ref('_3__0') }} 
  union all 
select * from {{ ref('_3__1') }} 
  union all 
select * from {{ ref('_3__2') }} 
  union all 
select * from {{ ref('_3__3') }} 
  union all 
select 1 as dummmy_column_1 
