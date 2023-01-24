select * from {{ ref('_4__1') }}
  union all 
select * from {{ ref('_4__2') }}