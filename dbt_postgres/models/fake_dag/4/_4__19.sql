select * from {{ ref('_3__19') }} 
  union all 
select * from {{ ref('_3__20') }} 
  union all 
select * from {{ ref('_3__21') }} 
  union all 
select 1 as dummmy_column_1 
