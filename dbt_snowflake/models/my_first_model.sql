
select 'hello, world!' as col,
'{{ this }}' as this,
'{{ this.name }}' as this_name,
{{ dbt_utils.get_filtered_columns_in_relation( this ) }} as cols2
