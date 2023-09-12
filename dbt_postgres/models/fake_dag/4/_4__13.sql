select * from {{ ref('_3__13') }} 
  union all 
select * from {{ ref('_3__14') }} 
  union all 
select * from {{ ref('_3__15') }} 
  union all 
select * from {{ ref('_3__16') }} 
  union all 
select 1 as dummmy_column_1 
