select * from {{ ref('_3__22') }} 
  union all 
select * from {{ ref('_3__23') }} 
  union all 
select * from {{ ref('_3__24') }} 
  union all 
select * from {{ ref('_3__25') }} 
  union all 
select 1 as dummmy_column_1 
