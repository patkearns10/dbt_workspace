select * from {{ ref('_3__40') }} 
  union all 
select * from {{ ref('_3__41') }} 
  union all 
select * from {{ ref('_3__42') }} 
  union all 
select * from {{ ref('_3__43') }} 
  union all 
select 1 as dummmy_column_1 
