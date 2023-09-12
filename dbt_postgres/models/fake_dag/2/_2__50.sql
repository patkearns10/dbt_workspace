select * from {{ ref('_1__50') }} 
  union all 
select * from {{ ref('_1__51') }} 
  union all 
select * from {{ ref('_0__81') }} 
  union all 
select 1 as dummmy_column_1 
