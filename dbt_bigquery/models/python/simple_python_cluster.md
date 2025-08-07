def model(dbt, session):
    dbt.config(
        materialized = "table",
        submission_method="cluster",
        dataproc_cluster_name="cse-jeryeo-cluster"
    )

    df = dbt.ref("my_first_dbt_model")

    # return final dataset (Pandas DataFrame)
    return df