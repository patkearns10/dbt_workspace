select * from {{ ref('_1__52') }} 
  union all 
select * from {{ ref('_1__53') }} 
  union all 
select * from {{ ref('_1__54') }} 
  union all 
select 1 as dummmy_column_1 
