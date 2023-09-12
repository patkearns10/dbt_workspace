select * from {{ ref('_2__0') }} 
  union all 
select * from {{ ref('_2__1') }} 
  union all 
select * from {{ ref('_2__2') }} 
  union all 
select * from {{ ref('_2__3') }} 
  union all 
select 1 as dummmy_column_1 
