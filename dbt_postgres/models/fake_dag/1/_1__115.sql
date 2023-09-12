select * from {{ ref('_0__99') }} 
  union all 
select 1 as dummmy_column_1 
