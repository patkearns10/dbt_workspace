{{ dbt_utils.unpivot(
    relation=source('dbt_pkearns', 'CORE_PLG_EXEC_METRICS'), 
    exclude=['period', 'period_type', 'first_day_of_period', 'last_day_of_period'], 
    cast_to='variant',
    field_name='METRIC', 
    value_name='VALUE') }}