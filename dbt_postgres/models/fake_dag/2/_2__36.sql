select * from {{ ref('_1__36') }} 
  union all 
select * from {{ ref('_1__37') }} 
  union all 
select * from {{ ref('_0__7') }} 
  union all 
select 1 as dummmy_column_1 
