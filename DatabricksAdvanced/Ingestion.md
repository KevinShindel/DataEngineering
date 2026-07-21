
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

##  Default Flow vs. Explicit Flow
> Most of the time, a flow is created automatically and implicitly when you define a streaming table or materialized view. This is the default flow — it shares the name of its target table.
> You can also create explicit flows separately from the table definition. This is required when you need to write to an existing table from a new source, or when multiple sources need to converge into one target.

```sql
-- Default Flow (implicit)
-- Table and flow created in one step. The flow takes the name of the table.
CREATE OR REFRESH STREAMING TABLE target_table
AS SELECT *
FROM STREAM source_table;
```

```sql
-- Explicit Flow (separate definition)
-- Table defined first. Flow defined separately and attached to the target by name.
CREATE OR REFRESH STREAMING TABLE target_table;
CREATE FLOW my_flow
AS INSERT INTO target_table BY NAME
SELECT * FROM STREAM source_table;
```

Multi-Flow vs. UNION — Why It Matters
A common alternative to multi-flow is combining sources with a UNION clause inside a single streaming table definition. For incremental pipelines, this creates critical limitations.

❌ UNION Approach
All sources share a single checkpoint
Adding a new source requires a full refresh to reprocess everything
A failure in one source can block all others
Harder to track lineage per source
Complex error handling across multiple data sources
Limited scalability as source count increases

✅ Multi-Flow Approach
Each flow has its own independent checkpoint
New sources can be added without a full refresh
Flows are isolated — one source failure does not affect others
Clear per-source lineage and monitoring
Independent error handling and recovery per source
Better scalability and maintainability

### Data Quality Expectations

Defining Expectations on Streaming Tables
In Spark Declarative Pipelines, constraints are defined inline in the table's column definition block using CONSTRAINT ... EXPECT. There are three violation modes that control what happens when a record fails a rule:

🟢
WARN (default — no ON VIOLATION clause)
Invalid rows are kept in the table. A warning metric is logged in the pipeline event log. Use for monitoring without blocking data.
CONSTRAINT valid_field EXPECT (field IS NOT NULL)
🟡
DROP ROW
Invalid rows are removed from the table. Dropped records are counted in metrics but do not appear in the final dataset.
CONSTRAINT valid_qty EXPECT (qty > 0) ON VIOLATION DROP ROW
🔴
FAIL UPDATE
The entire pipeline update is halted when even one record violates the rule. Use for critical fields where any violation indicates a serious upstream issue.
CONSTRAINT not_null_id EXPECT (id IS NOT NULL) ON VIOLATION FAIL UPDATE


```sql
--  Expectation Implementation Example
-- Here's a comprehensive example showing how to implement data quality expectations on a streaming table:

CREATE OR REFRESH STREAMING TABLE streaming_table
  (
    CONSTRAINT valid_qty        EXPECT (qty >= 0)                     ON VIOLATION DROP ROW,
    CONSTRAINT valid_amount     EXPECT (total_amount >= 0)            ON VIOLATION DROP ROW,
    CONSTRAINT not_null_ts      EXPECT (order_timestamp IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_email      EXPECT (customer_email RLIKE '^[^@]+@[^@]+\\.[^@]+$'),
    CONSTRAINT reasonable_qty   EXPECT (qty <= 1000)                 ON VIOLATION WARN
  )
```

### Monitoring and Observability

```sql
-- Going Deeper — System Tables and Event Logs
-- The pipeline event log records one row per flow update, capturing both throughput metrics and per-constraint expectation results. You can query it directly to build custom monitoring dashboards or alert pipelines.
SELECT timestamp, table_name, output_rows,
       data_quality.expectations
FROM event_log("pipeline_id")
WHERE event_type = 'flow_progress'
  AND data_quality.expectations IS NOT NULL
ORDER BY timestamp DESC;
```

## Liquid Clustering in Spark Declarative Pipelines
- D1. What is Liquid Clustering?
Liquid Clustering is a data layout optimization technique in Delta Lake that replaces traditional Hive-style partitioning and Z-Ordering. It organizes data files based on clustering keys to improve query performance through efficient data skipping.

Evolution

> Hive Partitioning -> Z-Ordering -> Liquid Clustering

LC Features: 
1. **Incremental** - Optimizes only new or unclustered data; avoids rewriting already clustered files. Efficient for streaming and write-heavy workloads.
2. **Flexible** - Clustering keys can be updated anytime without full table rewrite. Adapts to evolving query patterns.
3. **Self-Tuning** - With CLUSTER BY AUTO, Databricks automatically selects optimal keys based on observed query usage.



```sql
-- AutoClustering
-- Best when you are unsure which columns will be most frequently filtered.
CREATE OR REFRESH STREAMING TABLE my_table
CLUSTER BY AUTO
AS SELECT * FROM STREAM source_table;
```

```sql
-- CLUSTER BY (columns)
-- Best when you have strong domain knowledge of your most common filter patterns.
CREATE OR REFRESH STREAMING TABLE my_table
CLUSTER BY (region, order_date)
AS SELECT * FROM STREAM source_table;
```

### CDC + SCD Type 

Review the code

1. CREATE FLOW customers_scd_type_2_flow AS - Creates a named flow (customers_scd_type_2_flow) that defines how CDC changes will be processed.
2. AUTO CDC INTO sdp_cdc_2_silver.customers_silver_scd2_demo - Applies the CDC logic to the target Silver table.
3. FROM STREAM sdp_cdc_1_bronze.customers_bronze_clean_demo - Reads the streaming source data that includes new inserts, updates, and deletes.
4. KEYS (customer_id) - Identifies the unique customer record by its primary key.
5. APPLY AS DELETE WHEN operation = "DELETE" - Ensures records with a delete operation are removed from the target.
6. SEQUENCE BY timestamp_datetime - Orders incoming records so late-arriving data is processed correctly.
7. COLUMNS * EXCEPT (timestamp, _rescued_data, operation) - Selects all columns from the source except metadata or system fields.
8. STORED AS SCD TYPE 2 - Specifies the Slowly Changing Dimension Type 2 method, which updates records in place, keeping historical versions.
9. NOTE: For more information view the Databricks documentation AUTO CDC INTO (Lakeflow Spark Declarative Pipelines).

```sql
-- b. Perform SCD Type 2 into the silver table
CREATE FLOW customers_scd_type_2_flow AS 
AUTO CDC INTO sdp_cdc_2_silver.customers_silver_scd2_demo  -- Target: Where processed records are stored
FROM STREAM sdp_cdc_1_bronze.customers_bronze_clean_demo   -- Source: Clean CDC records from Bronze layer
  KEYS (customer_id)                                       -- Primary key: Used to match records for updates/deletes
  APPLY AS DELETE WHEN operation = "DELETE"                -- Delete logic: Remove records marked as DELETE
  SEQUENCE BY timestamp_datetime                           -- Ordering: Ensures changes are applied in correct sequence
  COLUMNS * EXCEPT (timestamp, _rescued_data, operation)   -- Column selection: Include all except metadata fields
  STORED AS SCD TYPE 2;                                    -- SCD Type 2: Maintains historical versions with __START_AT and __END_AT
```

### Read CSV files with metadata 

```sql
CREATE TABLE sales_bronze AS
SELECT *,
       _metadata.file_modification_time as file_modification_time,
       _metadata.file_name as source_file,
       current_timestamp() as ingestion_time
   FROM read_files(
        '/Volumes/my_volume/folder1/sales.csv',
        format => "csv",
        sep => "|",
        header => true
        )
```


```python

df = (
    spark
    .read
    .option('header', True)
    .option('sep', '|')
    .option('rescuedDataColumn', '_rescued_data')
    .csv('/path/to/file.csv')
    
)
```

### Force schema for CSV file
- schema unmatched data stored at _rescue_data

```sql
SELECT *,
       _metadata.file_modification_time as file_modification_time,
       _metadata.file_name as source_file,
       current_timestamp() as ingestion_time
   FROM read_files(
        '/Volumes/my_volume/folder1/sales.csv',
        format => "csv",
        sep => "|",
        header => true,
        schema => '''
       order_id INT, email STRING, transaction)timestamp BIGINT''',
       rescueddatacolumn => '_rescued_data'
        )
```

### Force header for headerless files

```sql
SELECT 
    CAST(_rescured_data:_c0 AS BIGINT) AS order_id,
    *
   FROM read_files(
           '/Volumes/my_volume/folder1/sales.csv',
           format = > "csv",
           sep = > "|",
           header = > true,
        )
```


## Kafka Base64 decode workflow


```sql
CREATE TABLE kafka_raw_bronze_decoded AS
SELECT
    key AS encoded_key,
    CAST(
        UNBASE64(key) AS STRING
    ) AS decoded_key,
    value AS encoded_value,
    CAST(
        UNBASE64(value) AS STRING
    ) AS decoded_value,
    offset, partition, timestamp, topic
FROM
    read_files(
    '/Volumes/path/to/kafka/files',
    format => "json"
    )
```

## Example of unflattening by using access to field

```sql
CREATE OR REPLACE TABLE kafka_events_bronze_flattened AS
       SELECT 
           decoded_key,
           offset,
           partition,
           timestamp,
           topic,
           decoded_value:device,
           decoded_value:traffic_source,
           decoded_value:geo, -- contains another JSON string
           decoded_value:items -- a nested array of JSON formated strings
           FROM kafka_raw_bronze
```

## Enforce schema for JSON using STRUCT

```sql
CREATE OR REPLACE TABLE kafka_events_bronze_struct AS
       SELECT 
           * EXCEPT (decoded_value)
           FROM_JSON(
       decoded_value, -- JSON formated string column
       SCHEMA_OF_JSON(
       SELECT decoded_value FROM kafka_raw_bronze_decoded LIMIT 1
       )
           ) as value
           FROM kafka_raw_bronze_decoded;

SELECT 
    decoded_key,
    value.device as device,
    value.geo.city as city,
    value.items as items,
    ARRAY_SIZE(items) as number_elements_in_array

FROM kafka_events_bronze_struct LIMIT 5;
```

## Explode arrays
- to explode arrays use EXPLODE function
- if array is empty but you need this row use EXPLODE_OUTER

```sql
SELECT 
    decoded_key,
    ARRAY_SIZE(value.items) as array_size,
    EXPLODE(value.items) as item,
    value.items
FROM kafka_events_bronze_struct
```

## Variant ( parsing of json )
- Public preview
- Not working on Serverless v1.x
- Use parsing function without schema ( flexible )

```sql
CREATE TABLE kafka_variant_bronze AS
SELECT  
    decoded_value,
    offset,
    partition,
    timestamp,
    topic,
    PARSE_JSON(decoded_value) AS json_variant_value -- Convert into VARIANT data type
FROM kafka_events_bronze_decoded;

SELECT 
    json_variant_value,
    json_variant_value:device :: STRING, -- obtain value of device and cast as string
    json_variant_value:items,
    
FROM kafka_variant_bronze
```