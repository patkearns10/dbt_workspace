select * from {{ ref('_1__30') }} 
  union all 
select * from {{ ref('_1__31') }} 
  union all 
select * from {{ ref('_1__32') }} 
  union all 
select 1 as dummmy_column_1 
