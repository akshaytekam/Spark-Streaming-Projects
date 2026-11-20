BASE_PATH = (
    "/Volumes/workspace/late_data_demo/"
    "orders_volume/streaming/events/"
)

SCHEMA_PATH = (
    "/Volumes/workspace/late_data_demo/"
    "orders_volume/streaming/schema/"
)


from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BASE_PATH = (
    "/Volumes/workspace/late_data_demo/"
    "orders_volume/streaming/events/"
)

SCHEMA_PATH = (
    "/Volumes/workspace/late_data_demo/"
    "orders_volume/streaming/schema/"
)

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

@dp.table(name="stream_bronze_orders")
def stream_bronze_orders():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .schema(order_schema)
        .load(BASE_PATH)
    )

    return df

# --------------SILVER TABLE--------------------

@dp.table(name="stream_silver_orders")
def stream_silver_orders():

    df = (
        spark.readStream
        .option("ignoreDeletes", "true")
        .table("stream_bronze_orders")
    )

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

    # Validate records
    df = df.filter(
        F.col("event_id").isNotNull() &
        F.col("order_id").isNotNull() &
        F.col("event_time").isNotNull() &
        F.col("amount").isNotNull() &
        (F.col("amount") >= 0)
    )

    # Watermarking + deduplication
    df = (
        df
        .withWatermark(
            "event_time",
            "30 minutes"
        )
        .dropDuplicatesWithinWatermark(
            ["event_id"]
        )
    )

    return df

# --------------GOLD TABLE--------------------

@dp.table(name="stream_gold_order_metrics")
def stream_gold_order_metrics():

    df = (
        spark.readStream
        .option("ignoreDeletes", "true")
        .table("stream_silver_orders")
    )

    result = (
        df
        .withWatermark(
            "event_time",
            "30 minutes"
        )
        .groupBy(
            F.window(
                "event_time",
                "5 minutes"
            ).alias("order_window")
        )
        .agg(
            F.count("*").alias("total_events"),

            F.sum(
                F.when(
                    F.col("status") == "PAID",
                    1
                ).otherwise(0)
            ).alias("paid_orders"),

            F.sum(
                F.when(
                    F.col("status") == "PAID",
                    F.col("amount")
                ).otherwise(0)
            ).alias("total_revenue"),

            F.sum(
                F.when(
                    F.col("status") == "CANCELLED",
                    1
                ).otherwise(0)
            ).alias("cancelled_orders")
        )
        .select(
            F.col("order_window.start")
             .alias("window_start"),

            F.col("order_window.end")
             .alias("window_end"),

            "total_events",
            "paid_orders",
            "total_revenue",
            "cancelled_orders"
        )
    )

    return result

# -----------------RECONCIALATION------------------


@dp.materialized_view(
    name="stream_reconciled_silver_orders"
)
def stream_reconciled_silver_orders():

    df = spark.read.table(
        "stream_bronze_orders"
    )

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
        .filter(
            F.col("event_id").isNotNull() &
            F.col("event_time").isNotNull() &
            F.col("amount").isNotNull() &
            (F.col("amount") >= 0)
        )
    )

    w = (
        Window
        .partitionBy("event_id")
        .orderBy(F.col("ingest_time").desc())
    )

    return (
        df
        .withColumn(
            "rn",
            F.row_number().over(w)
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


@dp.materialized_view(
    name="stream_reconciled_gold_orders"
)
def stream_reconciled_gold_orders():

    df = spark.read.table(
        "stream_reconciled_silver_orders"
    )

    return (
        df
        .groupBy(
            F.date_trunc(
                "hour",
                F.col("event_time")
            ).alias("order_hour")
        )
        .agg(
            F.count("*").alias("total_events"),

            F.sum(
                F.when(
                    F.col("status") == "PAID",
                    F.col("amount")
                ).otherwise(0)
            ).alias("total_revenue")
        )
    )
