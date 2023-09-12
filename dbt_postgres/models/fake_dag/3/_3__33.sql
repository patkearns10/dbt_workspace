select * from {{ ref('_2__33') }} 
  union all 
select * from {{ ref('_1__77') }} 
  union all 
select 1 as dummmy_column_1 
