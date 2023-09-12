select * from {{ ref('_1__48') }} 
  union all 
select * from {{ ref('_1__49') }} 
  union all 
select * from {{ ref('_1__50') }} 
  union all 
select * from {{ ref('_0__21') }} 
  union all 
select 1 as dummmy_column_1 
