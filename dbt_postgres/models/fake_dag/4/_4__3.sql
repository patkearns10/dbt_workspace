select * from {{ ref('_3__3') }} 
  union all 
select * from {{ ref('_3__4') }} 
  union all 
select * from {{ ref('_3__5') }} 
  union all 
select * from {{ ref('_3__6') }} 
  union all 
select * from {{ ref('_2__31') }} 
  union all 
select 1 as dummmy_column_1 
