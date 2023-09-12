select * from {{ ref('_2__44') }} 
  union all 
select * from {{ ref('_2__45') }} 
  union all 
select * from {{ ref('_1__9') }} 
  union all 
select 1 as dummmy_column_1 
