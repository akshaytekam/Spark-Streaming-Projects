
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    upper,
    trim,
    to_timestamp
)

@dp.view(
    name="customers_cdc_clean"
)
def customers_cdc_clean():

    return (
        spark.readStream
        .table("customers_bronze")

        # Clean string columns
        .withColumn("operation", upper(trim(col("operation"))))
        .withColumn("city", trim(col("city")))
        .withColumn("email", trim(col("email")))

        # Convert event timestamp
        .withColumn(
            "event_timestamp",
            to_timestamp(col("event_timestamp"))
        )

        # Validate CDC events
        .filter(col("customer_id").isNotNull())
        .filter(col("sequence_num").isNotNull())
        .filter(
            col("operation").isin(
                "INSERT", "UPDATE", "DELETE"
            )
        )
    )


# Silver streaming table: current state of each customer
dp.create_streaming_table(
    name="customers_silver",
    comment="Current customer state from CDC events"
)

# Apply CDC events to the silver target
dp.create_auto_cdc_flow(
    target="customers_silver",
    source="customers_cdc_clean",
    keys=["customer_id"],
    sequence_by="sequence_num",
    apply_as_deletes="operation = 'DELETE'"
)
