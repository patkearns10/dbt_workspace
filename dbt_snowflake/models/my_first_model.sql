
select 'hello, world!' as col,
'{{ this }}' as this,
'{{ this.name }}' as this_name,
{{ dbt_utils.get_filtered_columns_in_relation( this ) }} as cols2,
'{{ edw_get_column_list_new( this ) }}' as cols3,
{{ edw_get_quoted_column_list_new( this ) }} as cols4,
{{ edw_get_md5_column_list_new( this ) }} as cols5

