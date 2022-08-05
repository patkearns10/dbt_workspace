select 
    'hello, world!' as test_col,
    '{{ this }}' as this,
    '{{ this.name }}' as this_name,
    {{ dbt_utils.get_filtered_columns_in_relation(from=ref('my_first_model')) }} as utils_get_filtered_columns