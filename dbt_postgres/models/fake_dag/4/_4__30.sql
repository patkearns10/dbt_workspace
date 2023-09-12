select * from {{ ref('_3__30') }} 
  union all 
select * from {{ ref('_2__62') }} 
  union all 
select 1 as dummmy_column_1 
