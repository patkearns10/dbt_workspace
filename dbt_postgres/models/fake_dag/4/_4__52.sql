select * from {{ ref('_3__54') }} 
  union all 
select * from {{ ref('_3__55') }} 
  union all 
select 1 as dummmy_column_1 
