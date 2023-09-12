select * from {{ ref('_1__0') }} 
  union all 
select * from {{ ref('_1__1') }} 
  union all 
select 1 as dummmy_column_1 
