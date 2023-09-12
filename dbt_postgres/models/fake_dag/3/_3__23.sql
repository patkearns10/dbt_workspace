select * from {{ ref('_2__23') }} 
  union all 
select * from {{ ref('_2__24') }} 
  union all 
select * from {{ ref('_2__25') }} 
  union all 
select * from {{ ref('_2__26') }} 
  union all 
select 1 as dummmy_column_1 
