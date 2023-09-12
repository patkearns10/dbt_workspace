select * from {{ ref('_3__35') }} 
  union all 
select * from {{ ref('_3__36') }} 
  union all 
select * from {{ ref('_2__50') }} 
  union all 
select 1 as dummmy_column_1 
