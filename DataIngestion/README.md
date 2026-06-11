
## Batch Processing

```python
spark.read.load(
    "s3://bucket-name/path/to/data",
    format="parquet",
    header=True,
    inferSchema=True
)
```

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

```sql
CREATE TABLE table_name; 
COPY INTO table_name
FROM "s3://bucket-name/path/to/data"
FILEFORMAT = 'parquet' -- csv, json, avro, xml, txt etc
FORMAT_OPTIONS ('header' 'true', 'inferSchema' 'true') -- for csv only
COPY_OPTIONS ('mergeSchema' 'true') -- for parquet only
```

## Incremental Processing

```python
spark.readStream.load(
    "s3://bucket-name/path/to/data",
    format="parquet",
    header=True,
    inferSchema=True
    # AutoLoader with timed trigger can be used for incremental processing
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

## Streaming Processing

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