select * from {{ ref('_3__3') }}
  union all 
select * from {{ ref('_3__4') }}