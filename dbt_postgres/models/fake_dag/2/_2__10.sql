select * from {{ ref('_1__10') }} 
  union all 
select * from {{ ref('_1__11') }} 
  union all 
select 1 as dummmy_column_1 
