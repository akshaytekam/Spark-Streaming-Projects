from pyspark import pipelines as dp
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)

# Define source schema
customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("sequence_num", IntegerType(), False),
    StructField("event_timestamp", StringType(), True)
])

# Bronze streaming table
@dp.table(
    name="customers_bronze",
    comment="Raw customer CDC events from CSV files"
)
def customers_bronze():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(customer_schema)
        .load("/Volumes/workspace/cdc_lab/raw/")
    )
