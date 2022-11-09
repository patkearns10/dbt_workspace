"""
UDFs
    You can use the @udf decorator or udf function to define an "anonymous" function
    and call it within your model function's DataFrame transformation. 
"""

import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
import numpy

def register_udf_add_random():
    add_random = F.udf(
        # use 'lambda' syntax, for simple functional behavior
        lambda x: x + numpy.random.randint(1,99),
        return_type=T.FloatType(),
        input_types=[T.FloatType()]
    )
    return add_random

def model(dbt, session):

    dbt.config(
        materialized = "table",
        packages = ["numpy"]
    )

    payments_glitch = dbt.ref("define_function__payment_glitch")

    add_random = register_udf_add_random()

    # add money, who knows by how much
    df = payments_glitch.withColumn("amount_plus_random", add_random("amount"))

    return df
