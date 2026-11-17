# Databricks CDC Implementations

## For BATCH Processing:
- Full Snapshot (Full Outer Join + Merge Into)
- Delta Lake Merge (Sequence Number + Merge Into)
- PySpark CDC (Delta API + Merge Statement)

## For Streaming Processing:
- Auto Loader + foreachBatch()
- Lakeflow Auto CDC
