# Lakeflow Declarative Pipeline Auto CDC

**Interview questions you should know**

Here are 10 important questions based on what we've implemented.

1. What is Databricks AUTO CDC?

It is a Lakeflow Declarative Pipelines feature that automatically processes CDC events and maintains target tables using SCD Type 1, SCD Type 2, or bitemporal history.

2. What is the difference between CDC and SCD?

CDC captures changes from a source system. SCD defines how those changes are maintained in a target table.

3. What is the purpose of sequence_by?

It determines the logical order in which incoming CDC events should be applied.

4. What is the purpose of keys?

It identifies the business key used to match incoming changes to existing records.

5. How does AUTO CDC handle deletes?

Using apply_as_deletes, we specify which events represent deletions. With SCD Type 2, the active historical version is closed.

6. What are __START_AT and __END_AT?

They identify the beginning and ending sequence values for each historical record version.

7. Does AUTO CDC handle out-of-order records?

Yes. It uses sequence_by to determine the correct logical ordering.

8. What is the difference between SCD Type 1 and Type 2?

Type 1 overwrites existing values. Type 2 preserves historical versions.

9. What is the difference between AUTO CDC and AUTO CDC FROM SNAPSHOT?

AUTO CDC processes a change feed. AUTO CDC FROM SNAPSHOT derives changes by comparing successive source snapshots.

10. Why use AUTO CDC instead of manually implementing MERGE?

It simplifies CDC processing, handles sequencing and historical versioning, and reduces the amount of custom maintenance logic.
