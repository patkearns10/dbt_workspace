{{ printing(this=this) }}

select 1 as col
{{ printing(this=this) }}
union all
{{ log(">>>>>>>>>logging: " ~ this, info=True) }}
select 2 as col
