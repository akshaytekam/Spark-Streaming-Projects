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
