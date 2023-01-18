select * from {{ ref('_2__1') }}
  union all 
select * from {{ ref('_2__2') }}