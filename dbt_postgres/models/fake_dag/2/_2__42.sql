select * from {{ ref('_1__42') }} 
  union all 
select * from {{ ref('_1__43') }} 
  union all 
select * from {{ ref('_1__44') }} 
  union all 
select * from {{ ref('_1__45') }} 
  union all 
select * from {{ ref('_0__22') }} 
  union all 
select 1 as dummmy_column_1 
