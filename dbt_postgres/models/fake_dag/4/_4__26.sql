select * from {{ ref('_3__26') }} 
  union all 
select * from {{ ref('_3__27') }} 
  union all 
select * from {{ ref('_3__28') }} 
  union all 
select * from {{ ref('_3__29') }} 
  union all 
select * from {{ ref('_2__16') }} 
  union all 
select 1 as dummmy_column_1 
