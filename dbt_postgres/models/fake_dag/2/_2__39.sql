select * from {{ ref('_1__39') }} 
  union all 
select * from {{ ref('_1__40') }} 
  union all 
select * from {{ ref('_1__41') }} 
  union all 
select * from {{ ref('_1__42') }} 
  union all 
select 1 as dummmy_column_1 
