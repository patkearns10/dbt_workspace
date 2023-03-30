{{ dbt_utils.date_spine(
    datepart="minute",
    start_date="cast('2019-01-01' as date)",
    end_date="cast('2019-01-02' as date)"
   )
}}