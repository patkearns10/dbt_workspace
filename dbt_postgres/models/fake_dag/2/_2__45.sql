select * from {{ ref('_1__45') }} 
  union all 
select * from {{ ref('_0__92') }} 
  union all 
select 1 as dummmy_column_1 
