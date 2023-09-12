select * from {{ ref('_1__3') }} 
  union all 
select * from {{ ref('_1__4') }} 
  union all 
select * from {{ ref('_1__5') }} 
  union all 
select * from {{ ref('_1__6') }} 
  union all 
select 1 as dummmy_column_1 
