from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# Step 1: Create the SCD Type 2 target
dp.create_streaming_table(
    name="customers_history",
    comment="Customer history maintained using AUTO CDC SCD Type 2"
)

# Step 2: Apply incoming CDC events
dp.create_auto_cdc_flow(
    target="customers_history",

    source="customers_cdc_clean",

    # Business key
    keys=["customer_id"],

    # Logical ordering of CDC events
    sequence_by=col("sequence_num"),

    # Identify delete events
    apply_as_deletes=expr(
        "operation = 'DELETE'"
    ),

    # Exclude CDC metadata
    except_column_list=[
        "operation",
        "sequence_num",
        "event_timestamp"
    ],

    # Maintain historical versions
    stored_as_scd_type="2"
)
