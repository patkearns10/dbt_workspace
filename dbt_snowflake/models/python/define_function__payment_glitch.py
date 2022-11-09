def times_two(x):
    return x * 2

def model(dbt, session):
    dbt.config(materialized="table")

    # bring in reference model as dataframe
    payments_glitch = dbt.ref("stg_payments")

    # apply infinite money glitch
    df = payments_glitch.withColumn("amount__glitch", times_two(payments_glitch["amount"]))
    return df
