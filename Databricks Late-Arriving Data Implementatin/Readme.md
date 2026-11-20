# Databricks Late-Arriving Data Implementatin

## A. Watermarking:
A watermark tells Spark how long it should wait for late-arriving records during stateful streaming operations.
```text
df.withWatermark("event_time", "30 minutes")
```

## B. Deduplication:
Kafka and other message brokers can deliver the same event more than once.
```text
df.dropDuplicates(["event_id"])  #Batch
(or)
df.dropDuplicatesWithinWatermark(["event_id"])  #Streaming
```

## C. Reconciliation:
What happens if a record arrives after our 30-minute watermark?

We don't want to permanently lose it.

Our strategy:
1. Preserve every received event in Bronze.
2. Apply watermarking and deduplication in the normal streaming path.
3. Run a periodic reconciliation process against the retained Bronze data.
4. Recalculate affected historical aggregates.
5. Correct the Gold tables.
This gives us two layers of protection:
- Streaming processing for low latency.
- Batch reconciliation for completeness.

# ------------------------------------------------------------------
## Understand our batch scenario:
The question is:

How do we ensure that the 11:00 AM batch updates the 10:00 AM business results?

Our solution is to read the complete available landing dataset during each batch refresh, deduplicate by event_id, and calculate the results using event_time. Because our Bronze materialized view reads the available files again, and Silver and Gold are recomputed from Bronze, the historical event will be incorporated into the appropriate hourly result.

*This is the key batch-processing technique:*
Reprocess the relevant historical data or recompute affected partitions so late-arriving records are reflected in the analytical tables.

For larger production datasets, we would avoid full refreshes and use incremental ingestion(CDC) with a reliable event key and an idempotent Delta MERGE/upsert strategy. This prevents unnecessarily reprocessing the entire history.

# ------------------------------------------------------------------
## Streaming Processing scenario:

```text
E-commerce Application
        |
        v
       Kafka
        |
        v
Databricks Streaming Ingestion
        |
        v
Lakeflow Declarative Pipeline
        |
        +------ Bronze
        |
        +------ Silver
        |        |
        |        +-- Event-time validation
        |        +-- Watermarking
        |        +-- Deduplication
        |
        +------ Gold
                 |
                 +-- 5-minute order aggregation
                 +-- Revenue calculation
```
We will implement:
1. Timestamp conversion.
2. Data validation.
3. Watermarking.
4. Streaming deduplication.




