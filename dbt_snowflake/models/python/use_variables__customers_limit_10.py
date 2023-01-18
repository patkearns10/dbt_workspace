def model(dbt, session):
    target_name = dbt.config.get("target_name")

    # bring in reference model as dataframe
    customers_df = dbt.ref("customers")

    # limit data in dev
    if target_name == "dev":
        customers_df = customers_df.limit(10)

    # return dataframe
    return customers_df
    