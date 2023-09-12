select * from {{ ref('_2__32') }} 
  union all 
select * from {{ ref('_2__33') }} 
  union all 
select 1 as dummmy_column_1 
