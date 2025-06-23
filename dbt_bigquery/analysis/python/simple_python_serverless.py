def model(dbt, session):
    dbt.config(
        materialized = "table",
        submission_method="serverless"
    )

    df = dbt.ref("my_first_dbt_model")

    # return final dataset (Pandas DataFrame)
    return df