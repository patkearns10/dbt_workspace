select * from {{ ref('_3__6') }} 
  union all 
select * from {{ ref('_2__49') }} 
  union all 
select 1 as dummmy_column_1 
