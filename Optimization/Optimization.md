
## Table Optimization

```sql
OPTIMIZE my_table_name ZORDER BY (date)
```

## Log Cleaning Operations

```sql
VACUUM my_table_name RETAIN 168 HOURS;
```

```sql
ALTER TABLE my_table_name SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    )
    -- ensures that files are written and compacted efficiently without run OPTIMIZE manually
```


## Partitioning 

- groupBy - automatically repartition-by-keys
- df.repartition(n, F.col('column_name')) - repartition on another key
- Target - 100-200 Mb per partition
- Num of partitions should not be less tha number of cores 
- df.repartition / df.coalesce - depends on problem
- monitor task duration ( aim for 50-200 ms)
- Default table partition ( spark.sql.('DESCRIBE DETAIL table_name').select('numFiles') )

## Caching

- On multiple accesses to same DF
- Expensive transformations upstream
- Interactive analysis and ML iterations
- Use df.cache \ df.persist explicitly
- monitor executor memory usage with UI
- Call df.unpersist when no longer needed

## Join Optimizations

- smaller df should be referenced first ( small_df.join(large_df), 'key')
- broadcast for small dataframes
- repartition by join keys
- select required columns only
- filter first !
- use df.explain to view plan
- Explain support next command
- - df.explaind(mode='formatted')
- - df.explaind(mode='extended')


```python

def show_partition_sizes(df: pyspark.sql.DataFrame):
    # define the ouput dir
    output_path = 'catalog.schema.temp_dataframe'
    
    # delete dir if already exists
    dbutils.fs.rm(output_path, True)
    
    # write df as parquet files
    df.write.format('parquet').option('compression', 'none').mode('overwrite').save(output_path)
    
    # list the files in dir
    files = dbutils.fs.ls(output_path)
    files = filter(lambda x: x.path.endswith('.parquet'), files)
    for i, file in enumarate(files):
        print(f'Partition: {i}, Size: {file.size}')

```