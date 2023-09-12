select * from {{ ref('_2__25') }} 
  union all 
select * from {{ ref('_2__26') }} 
  union all 
select * from {{ ref('_2__27') }} 
  union all 
select 1 as dummmy_column_1 
