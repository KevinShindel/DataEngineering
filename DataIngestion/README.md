## Difference between read_files and cloud_files

| Feature                             | `read_files`               | `cloud_files`                                    |
|-------------------------------------|----------------------------|--------------------------------------------------|
| Purpose                             | Read files from storage    | Incrementally ingest new files using Auto Loader |
| Streaming Support                   | Batch and streaming syntax | Designed for streaming (Auto Loader)             |
| Tracks processed files              | ❌ No                       | ✅ Yes (checkpoint/state)                         |
| Detects newly arrived files         | ❌ No                       | ✅ Yes                                            |
| Schema evolution                    | Basic                      | Advanced automatic schema evolution              |
| File discovery                      | Directory listing          | Optimized directory listing / notifications      |
| Scalability                         | Small to medium datasets   | Very large datasets (millions/billions of files) |
| Recommended for Bronze ingestion    | Sometimes                  | Yes                                              |
| Uses Auto Loader                    | No                         | Yes                                              |
| Supports file notification services | No                         | Yes (AWS SQS, Azure Queue, GCP Pub/Sub)          |

## Performance evaluation

| Scenario         | read_files | cloud_files |
|------------------|------------|-------------|
| 100 files        | ⭐⭐⭐⭐⭐      | ⭐⭐⭐⭐        |
| 10,000 files     | ⭐⭐⭐        | ⭐⭐⭐⭐⭐       |
| 10 million files | ⭐          | ⭐⭐⭐⭐⭐       |

## Decision making

| Scenario                                   | CTAS | COPY INTO | Streaming Table | MERGE INTO |
|--------------------------------------------|------|-----------|-----------------|------------|
| One-time historical load                   | ✅    | ❌         | ❌               | ❌          |
| Load only new files                        | ❌    | ✅         | ✅               | ❌          |
| Continuous/managed ingestion               | ❌    | ❌         | ✅               | ❌          |
| Insert new rows only                       | ⚠️   | ✅         | ✅               | ⚠️         |
| Update existing rows                       | ❌    | ❌         | ❌               | ✅          |
| Implement Slowly Changing Dimensions (SCD) | ❌    | ❌         | ❌               | ✅          |
| Bronze ingestion                           | ⚠️   | ✅         | ✅ Best          | ❌          |
| Silver upsert/deduplication                | ❌    | ❌         | ⚠️              | ✅ Best     |

| Requirement                                | CTAS             | COPY INTO | Streaming Table |
|--------------------------------------------|------------------|-----------|-----------------|
| One-time historical load                   | ✅ Excellent      | ❌         | ❌               |
| Full refresh every run                     | ✅ Excellent      | ❌         | ❌               |
| Incremental file ingestion                 | ❌                | ✅         | ✅               |
| Handles schema evolution well              | ❌                | Limited   | ✅               |
| Production Medallion pipeline              | ❌                | Possible  | ✅ Best fit      |
| Automatic downstream dependency management | ❌                | ❌         | ✅               |
| Data quality expectations                  | ❌                | ❌         | ✅               |
| Can run on a weekly schedule               | ⚠️ (full reload) | ✅         | ✅               |




## Batch Processing

```python
spark.read.load(
    "s3://bucket-name/path/to/data",
    format="parquet",
    header=True,
    inferSchema=True
)
```
#### CTAS

- create delta table by default from files
- JSON, CSV, XML, TEXT, BINARYFILE, PARQUET, AVRO, ORC

```sql
CREATE TABLE new_table AS 
SELECT *
FROM read_files(
    "s3://bucket-name/path/to/data",
    format="parquet", -- csv, json, avro, xml, txt etc.
    header=True, -- for csv only
    inferSchema=True -- for csv only
)
```

#### COPY INTO

- legacy method for Incremental batch processing

```sql
CREATE TABLE table_name; 
COPY INTO table_name
FROM "s3://bucket-name/path/to/data"
FILEFORMAT = 'parquet' -- csv, json, avro, xml, txt etc
FORMAT_OPTIONS ('header' 'true', 'inferSchema' 'true') -- for csv only
COPY_OPTIONS ('mergeSchema' 'true') -- for parquet only
```

## Incremental Processing
- Only new data is ingested

```python
spark.readStream.load(
    "s3://bucket-name/path/to/data",
    format="parquet",
    header=True,
    inferSchema=True
    # AutoLoader with timed trigger can be used for incremental processing
)
```

#### Auto-Loader
- for Incremental batch or Streaming

```sql
CREATE OR REFRESH STREAMING TABLE streaming_table 
       SCHEDULE EVERY 1 HOUR
       AS 
       SELECT *
       FROM STREAM read_files(
                              "s3://bucket-name/path/to/data",
                              format="parquet", -- csv, json, avro, xml, txt etc.
           )
```

## Streaming Processing
- Continuously load data
- Micro-batch
- Frequent interval

```python
(spark
 .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "s3://bucket-name/path/to/schema")
    .load("s3://bucket-name/path/to/data")
.writeStream
    .format("delta")
    .option("checkpointLocation", "s3://bucket-name/path/to/checkpoint")
    .trigger(processingTime="1 minute")
    .toTable("catalog.database.streaming_table")
)
```

```sql
CREATE OR REFRESH STREAMING TABLE streaming_table 
       SCHEDULE EVERY 1 HOUR
       AS 
       SELECT *
       FROM STREAM read_files(
                              "s3://bucket-name/path/to/data",
                              format="parquet", -- csv, json, avro, xml, txt etc.
           )
```

## Read from files

```sql
SELECT * 
FROM parquet.`/Volumes/data/file.parquet`
```
# Formats: CSV, AVRO, JSON, XML, TXT, PARQUET, ORC, DELTA, etc.
```sql
SELECT *
FROM read_files(
    "/Volumes/data/file.parquet",
    format="parquet", -- csv, json, avro, xml, txt etc.
    header=True, -- for csv only
    inferSchema=True -- for csv only)
)
```

## Incremental Data Ingestion


### Example 1: table with schema

```sql
-- Create a table to store the ingested data
DROP TABLE IF EXISTS table_name_ci;
-- Create the table with the appropriate schema
CREATE TABLE table_name_ci (
    id INT,
    name STRING,
    age INT,
    PRIMARY KEY (id)
);
-- Ingest data from the source file into the table
COPY INTO table_name_ci
FROM `/Volumes/data/file.csv`
FILEFORMAT = 'csv'
FORMAT_OPTIONS ('header' 'true', 'inferSchema' 'true')
COPY_OPTIONS ('mergeSchema' 'true')
```

### Example 2: table without schema

```sql
-- Create a table to store the ingested data
DROP TABLE IF EXISTS table_name_ci;
-- Create the table without a predefined schema
CREATE TABLE table_name_ci;
-- Ingest data from the source file into the table
COPY INTO table_name_ci
FROM `/Volumes/data/file.csv`
FILEFORMAT = 'csv'
FORMAT_OPTIONS ('header' 'true', 'inferSchema' 'true')
COPY_OPTIONS ('mergeSchema' 'true')
```


## Metadata appending

```sql
CREATE TABLE table_name_ci AS
SELECT *,
    current_timestamp() AS ingestion_time,
    input_file_name() AS source_file,
    current_user() AS ingested_by
    file_modified_time() AS file_modified_time,
 CAST(FROM_UNIXTIME(user_first_touch_timestamp/1000000) AS DATE ) AS first_touch_date
FROM read_files(
    "/Volumes/data/file.csv",
    format="csv",
    header=True,
    inferSchema=True    
     )
     LIMIT 10
 );
```

### Rescued Data

```sql
SELECT 
    CAST(_rescued_data:_c0 AS BIGINT) AS order_id,
    *
FROM read_files(
    "/Volumes/data/file.csv",
    format="csv",
    header=True,
    inferSchema=True,
    rescuedDataColumn="_rescued_data"
    )
```   