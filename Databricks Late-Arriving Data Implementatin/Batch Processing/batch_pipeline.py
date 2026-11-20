
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BASE_PATH = "/Volumes/workspace/late_data_demo/orders_volume/batch/"

order_schema = """
    event_id STRING,
    order_id STRING,
    customer_id STRING,
    event_time STRING,
    ingest_time STRING,
    amount DOUBLE,
    status STRING,
    channel STRING
"""

@dp.materialized_view(name="batch_bronze_orders")
def batch_bronze_orders():

    df = (
        spark.read
        .schema(order_schema)
        .option("header", "true")
        .csv(BASE_PATH)
    )

    return df

# ---------------SILVER TABLE-----------------

@dp.materialized_view(name="batch_silver_orders")
def batch_silver_orders():

    df = spark.read.table("batch_bronze_orders")

    # Convert timestamps
    df = (
        df
        .withColumn(
            "event_time",
            F.to_timestamp("event_time")
        )
        .withColumn(
            "ingest_time",
            F.to_timestamp("ingest_time")
        )
    )

    # Basic validation
    df = df.filter(
        F.col("event_id").isNotNull() &
        F.col("order_id").isNotNull() &
        F.col("event_time").isNotNull() &
        F.col("amount").isNotNull() &
        (F.col("amount") >= 0)
    )

    # Deduplication
    window_spec = (
        Window
        .partitionBy("event_id")
        .orderBy(F.col("ingest_time").desc())
    )

    df = (
        df
        .withColumn(
            "row_num",
            F.row_number().over(window_spec)
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    return df

# ---------------GOLD TABLE------------------


@dp.materialized_view(name="batch_gold_hourly_orders")
def batch_gold_hourly_orders():

    df = spark.read.table("batch_silver_orders")

    result = (
        df
        .groupBy(
            F.date_trunc(
                "hour",
                F.col("event_time")
            ).alias("order_hour")
        )
        .agg(
            F.countDistinct("order_id")
             .alias("total_orders"),

            F.sum(
                F.when(
                    F.col("status") == "PAID",
                    F.col("amount")
                ).otherwise(0)
            ).alias("total_revenue"),

            F.countDistinct(
                F.when(
                    F.col("status") == "PAID",
                    F.col("order_id")
                )
            ).alias("paid_orders"),

            F.countDistinct(
                F.when(
                    F.col("status") == "CANCELLED",
                    F.col("order_id")
                )
            ).alias("cancelled_orders")
        )
    )

    return result
