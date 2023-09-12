select * from {{ ref('_1__11') }} 
  union all 
select * from {{ ref('_1__12') }} 
  union all 
select * from {{ ref('_0__9') }} 
  union all 
select 1 as dummmy_column_1 
