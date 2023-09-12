select * from {{ ref('_2__51') }} 
  union all 
select * from {{ ref('_2__52') }} 
  union all 
select 1 as dummmy_column_1 
