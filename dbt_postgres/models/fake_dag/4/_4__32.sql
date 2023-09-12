select * from {{ ref('_3__32') }} 
  union all 
select * from {{ ref('_2__63') }} 
  union all 
select 1 as dummmy_column_1 
