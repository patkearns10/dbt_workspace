select * from {{ ref('_3__16') }} 
  union all 
select * from {{ ref('_2__31') }} 
  union all 
select 1 as dummmy_column_1 
