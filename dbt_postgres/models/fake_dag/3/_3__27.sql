select * from {{ ref('_2__27') }} 
  union all 
select * from {{ ref('_2__28') }} 
  union all 
select * from {{ ref('_2__29') }} 
  union all 
select 1 as dummmy_column_1 
