select * from {{ ref('_2__3') }}
  union all 
select * from {{ ref('_2__4') }}