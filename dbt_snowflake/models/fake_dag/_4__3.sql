select * from {{ ref('_3__5') }}
  union all 
select * from {{ ref('_3__6') }}