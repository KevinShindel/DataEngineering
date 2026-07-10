## [Configure Auto Loader streams in file notification mode](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/file-notification-mode)

## [Archiving files in the source directory to lower costs](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/production)

## [Common data loading patterns](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/patterns)

## [Auto Loader Options](https://docs.databricks.com/aws/en/spark/api-options#stream-reader-al)


> To process files that triggered file arrival triggers, you can use Auto Loader. <br />
> Auto Loader incrementally and efficiently processes new files with exactly-once guarantees. <br />
> For example, use the snippet below to load files into a Delta table. <br />
> To use this solution, create a job with a file-arrival trigger and add a notebook containing the code below. <br />
> Replace each [REPLACE] placeholder with the appropriate value. <br />

```python
# Configuration
file_location = "[REPLACE]" # The same URL configured for the file arrival trigger.
checkpoint_location = "[REPLACE]" # a separate URL (outside `file_location`) used to store the Auto Loader checkpoint, which enables exactly-once processing.
sink_table = "[REPLACE]" # Delta table to write to

# Use Auto Loader to discover new files.
# Do not modify code below this line
streamingQuery = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .option("cloudFiles.schemaLocation", checkpoint_location) \
  .option("cloudFiles.useManagedFileEvents","true") \
  .load(file_location) \
  .writeStream \
  .option("checkpointLocation", checkpoint_location) \
  .trigger(availableNow = True) \
  .toTable(sink_table)
```

> If you need to process new files with custom logic and only want to discover the URL for new files, you can use foreachBatch instead, as shown in the code snippet below.  <br />
> Note that foreachBatch provides only at-least-once processing guarantees.  <br />
> For more information on using foreachBatch, see Use foreachBatch to write to arbitrary data sinks <br />

```python
# Configuration
file_location = "[REPLACE]" # The same URL configured for the file arrival trigger.
checkpoint_location = "[REPLACE]" # a separate URL (outside `file_location`) used to store the Auto Loader checkpoint, which enables exactly-once processing.

def process_batch(batch_df, batch_id):
  file_url = batch_df.select("path").collect()[0].path
  # [REPLACE] Your custom function for processing newly arrived files


# Use Auto Loader to discover new files.
# Do not modify code below this line
streamingQuery = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "binaryFile") \
  .option("cloudFiles.useManagedFileEvents","true") \
  .load(file_location) \
  .drop("content") \
  .writeStream \
  .foreachBatch(process_batch) \
  .option("checkpointLocation", checkpoint_location) \
  .trigger(availableNow = True) \
  .start()
```