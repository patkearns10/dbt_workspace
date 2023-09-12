select * from {{ ref('_1__35') }} 
  union all 
select * from {{ ref('_1__36') }} 
  union all 
select * from {{ ref('_0__75') }} 
  union all 
select 1 as dummmy_column_1 
