select * from {{ ref('_2__15') }} 
  union all 
select 1 as dummmy_column_1 
