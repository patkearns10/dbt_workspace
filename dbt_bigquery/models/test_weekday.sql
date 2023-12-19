
-- {{ log('add_weekday_sale_suffix(): ' ~ add_weekday_sale_suffix(), info=True) }}
-- {{ log('table_name: ' ~ table_name, info=True) }}
-- {{ log('var weekday: ' ~ weekday, info=True) }}


{% set table_name =  add_weekday_sale_suffix("model") %}

-- 
--- {{ log('add_weekday_sale_suffix("model"): ' ~ add_weekday_sale_suffix("model"), info=True) }}
--- {{ log('table_name: ' ~ table_name, info=True) }}
-- {{ log('var weekday: ' ~ weekday, info=True) }}


{{
config(
alias=table_name,
)
}}

select 1 as col
