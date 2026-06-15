
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