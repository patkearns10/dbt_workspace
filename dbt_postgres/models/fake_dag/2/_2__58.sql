select * from {{ ref('_1__58') }} 
  union all 
select * from {{ ref('_0__25') }} 
  union all 
select 1 as dummmy_column_1 
