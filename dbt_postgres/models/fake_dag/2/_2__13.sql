select * from {{ ref('_1__13') }} 
  union all 
select * from {{ ref('_1__14') }} 
  union all 
select * from {{ ref('_1__15') }} 
  union all 
select * from {{ ref('_1__16') }} 
  union all 
select 1 as dummmy_column_1 
