select * from {{ ref('_3__18') }} 
  union all 
select * from {{ ref('_3__19') }} 
  union all 
select * from {{ ref('_3__20') }} 
  union all 
select * from {{ ref('_2__32') }} 
  union all 
select 1 as dummmy_column_1 
