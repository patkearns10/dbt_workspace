select * from {{ ref('_3__47') }} 
  union all 
select * from {{ ref('_3__48') }} 
  union all 
select * from {{ ref('_3__49') }} 
  union all 
select * from {{ ref('_3__50') }} 
  union all 
select * from {{ ref('_2__23') }} 
  union all 
select 1 as dummmy_column_1 
