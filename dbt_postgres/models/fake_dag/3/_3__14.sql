select * from {{ ref('_2__14') }} 
  union all 
select * from {{ ref('_1__55') }} 
  union all 
select 1 as dummmy_column_1 
