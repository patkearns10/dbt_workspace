select * from {{ ref('_3__11') }} 
  union all 
select * from {{ ref('_3__12') }} 
  union all 
select * from {{ ref('_3__13') }} 
  union all 
select * from {{ ref('_3__14') }} 
  union all 
select * from {{ ref('_2__24') }} 
  union all 
select 1 as dummmy_column_1 
