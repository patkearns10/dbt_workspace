import holidays

def is_holiday(date_col):
    usa_holidays = holidays.US()
    is_holiday = (date_col in usa_holidays)
    return is_holiday

def model(dbt, session):
    dbt.config(
        materialized = "table",
        packages = ["holidays"]
    )

    orders_df = dbt.ref("orders")

    df = orders_df.to_pandas()

    # apply our function
    # (columns need to be in uppercase on Snowpark)
    df["IS_HOLIDAY"] = df["ORDER_DATE"].apply(is_holiday)

    # return final dataset (Pandas DataFrame)
    return df
