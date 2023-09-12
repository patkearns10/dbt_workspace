select * from {{ ref('_2__48') }} 
  union all 
select * from {{ ref('_1__85') }} 
  union all 
select 1 as dummmy_column_1 
