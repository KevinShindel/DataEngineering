
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


## Performance Issues 

1. Performance bottlenecks can occur for several reasons. One is the small file
problem—when data is spread across many tiny files, opening and moving them
across the network slows queries and can even trigger cloud provider I/O throttling.

2. Another is data skew, where one partition is much larger than others or where
transformations create imbalance. Since processing waits for the slowest executor, a
single large partition can delay the entire job.

3. Bottlenecks also happen when processing more data than needed. Unlike
traditional data lakes that often rewrite entire datasets, we can process only the
required files and use techniques like data skipping to further improve performance.



### Avoiding the Small File Problem

Automatically handle this common performance challenge in Data Lakes
- Too many small files greatly increases overhead for reads
- Too few large files reduces parallelism on reads
- Over-partitioning is a common problem
- Databricks will automatically tune the size of Delta Lake tables
- Databricks will automatically compact small files on write with auto-optimize

<hr/>

### Z-Ordering

Z-ordering is a way to organize data within a table based on a specific column so that
similar values are stored together in the same files. Although Databricks currently
recommends liquid clustering over Z-ordering or partitioning for new tables, Z-ordering
is still sometimes useful. With Z-ordering, data is physically organized by the chosen
column, and after this process, each data file will record the minimum and maximum
values for that column in its metadata (for example, in the footer of a Parquet file).
This setup allows Spark, when running a query that filters on the Z-ordered column, to
check these min and max values and skip any files that cannot possibly contain the
required data, without even opening them. This reduces the number of files that must
be read and improves overall query performance, especially for queries filtering on
columns with many distinct values.

1. For effective data skipping and Z-ordering in Delta Lake, it’s necessary to collect
statistics on your data.

2. By default, Databricks Delta Lake gathers statistics for the first
32 columns in a table, recording values like min and max for each column in the file
metadata.

3. Filters are applied in order: partition filters, data
filters, and then pushed filters. Because of possible precision or truncation issues,
statistics for timestamp and string columns may not always lead to exact matches,
requiring a fallback to file scanning.

<hr/>

### Partitioning

1. Generally not recommended!
- Partitioning is usually misused/overused
- Tiny file problems or Skew

2. Good use-cases for partitioning
- Isolating data for separate schemas (single->multiplexing)
- GDPR/CCP use cases where you commonly delete a partitions worth of data
- Use cases requiring a physical boundary to isolate data SCD Type 2, partition on
current or not for better performance.

3. If you partition
- Choose column with low cardinality
- Try to keep each partition less than 1tb and greater than 1gb
- Tables expected to grow TBs
- Partition (usually) on a date, zorder on commonly used predicates in where clauses

> When partitioning is needed, it's best to use columns with low cardinality (few unique
values) to avoid generating many tiny files. Aim to keep each partition between 1 GB
and 1 TB. Partitioning is especially helpful for tables expected to grow above a
terabyte, and is commonly done on a date column. Z-ordering can also be used
together with partitioning to optimize queries that filter on frequently used columns in
WHERE clauses.

> While partitioning can be useful in some scenarios, Databricks currently recommends
liquid clustering as a more flexible and efficient approach. The main challenges with
partitioning include the risk of creating many small files, which increases metadata
overhead and slows down read operations. Additionally, partitioning can lead to data
skew, where some partitions contain very little data while others have a lot, resulting
in inconsistent file sizes. This imbalance makes query performance and optimization
less effective.

<hr/>

### Liquid Clustering 

Innovative technique to clustering data layout to support efficient query access and reduce data management and tuning overhead.
It’s flexible and adaptive to data pattern changes, scaling, and data skew.


**Benefits**

1. Best performance out of the box ( Clustering on write )
2. Most consistent data skipping (Immune to data skew)
3. Minimal write amplification on table maintenance (True incremental optimize)
4. Row Level Concurrency (Simplify logic of concurrent writers)
5. Reduced Cognitive Overhead (No worrying about cardinality)

<hr/>

### Table Statistics

> Keeping table statistics up to date for best results with cost-based optimizer


- Collects statistics on all columns in table
- Helps Adaptive Query Execution
- Choose proper join type
- Select correct build side in a hash-join
- Calibrating the join order in a multi-way join

```sql
ANALYZE TABLE mytable COMPUTE STATISTICS FOR ALL COLUMNS
```

> Table statistics are calculated for any of these optimization techniques and play a
crucial role in improving table performance. By collecting statistics on the table
columns, the system can optimize how data files are read and processed. These
statistics are especially helpful for Adaptive Query Execution (AQE), which uses them
to choose the best join type, select the appropriate build side in hash joins, and
optimize the join order in multi-way joins. To gather these statistics, it is possible to
use the ANALYZE TABLE command and specify 'compute statistics for all columns.'
These detailed statistics then support more efficient query planning and execution.


<hr/>

### Predictive Optimization

- Predictive optimization refers to using predictive analytics techniques to
automatically optimize and enhance the performance of systems, processes,
or workflow.
- It involves leveraging data-driven insights to proactively identify and
implement optimizations, improving efficiency, cost-effectiveness, and overall
system performance.

**Key Features**

1. Automatic Maintenance ( It automates the execution of background maintenance tasks on Delta tables. )
2. Set and Forget Approach (It intelligently and automatically runs maintenance jobs without requiring ongoing user supervision)
3. Support Maintenance Operations ( It supports maintenance operations, including **OPTIMIZE** to improve query performance by optimizing file sizes  and **VACUUM** to reduce storage costs by deleting unused data)
4. Serverless Computing (It utilizes serverless compute, eliminating the need for users to manually manage compute cluster. )

> Predictive optimization in Databricks provides automatic maintenance for Delta tables,
taking over routine optimization tasks that used to require manual effort. This includes
background automation of maintenance activities such as **OPTIMIZE** and **VACUUM**,
so there’s no need to worry about scheduling or supervising these jobs. 

<hr/>

## Code Optimization

<details>
<summary> 1. Skew </summary>

**Problems**

- OOM problems
- Long Evaluation
- Skewed partitions

**Solution**

- Use Adaptive Query Execution
> When your job has more than **2,000 shuffle** partitions, Spark can no
longer keep track of specific shuffle block sizes; instead, it only retains average sizes,
making it impossible for AQE to detect skew. You can either reduce the number of
shuffle partitions to **fewer than 2,000** or change the following Spark configuration to a
value greater than your shuffle partition count to resolve this problem

- Skew Hints
> In the case where you are able to identify the table, the column, and preferably also
the values that are causing data skew, then you can explicitly tell Spark about it using
skew hints so that Spark can try to resolve it for you.

- Filter Skewed values
> If it’s possible to filter out the values around which there is a skew, then that will easily
solve the issue. If you join using a column with a lot of null values, for example, you’ll
have data skew. In this scenario, filtering out the null values will resolve the issue.

- Salt the join keys
> It’s a strategy for breaking a large skewed partition into smaller partitions by
appending random integers as suffixes to skewed column values.

</details>

<details>
<summary> 2. Shuffle </summary>

#### Problem

**Job**: A job in Apache Spark refers to the overall computation that needs to be
executed on the data. It comprises one or more stages.

**Stage**: A stage is a collection of tasks that can be executed together. Stages are
formed based on the transformations applied to the data, and they represent a unit of
work.

**Task**: A task is the smallest unit of work in Spark. Each task performs an identical
operation across a partition of the data.

**Wide Transformation**: A wide transformation is an operation that requires two stages
to complete. It often involves shuffling, which is the process of redistributing data
across partitions. Examples of wide transformations include join(), distinct(),
groupBy(), orderBy(), and some actions like count().

**Narrow Transformation**: A narrow transformation is an operation that requires only
one stage to complete. Unlike wide transformations, narrow transformations do not
involve shuffling.

**Shuffle**: Shuffling is the act of moving data from the output of one stage to the input
of another. It is a side effect of wide transformations and is a critical operation that
involves redistributing and reorganizing data.


#### Solution

 - Reduce network IO by using fewer,larger workers
 - Speed up shuffle reads & writes by using NVMe & SSDs
 - Reduce amount of shuffled data
 - Remove unnecessary columns
 - Filter out unnecessary records preemptively
 - Denormalize datasets, esp when shuffle is rooted in a join
 - Reordering Join Strategies
 - Broadcast Hash Join
 - Shuffle Hash Joins (default for Databricks Photon)
 - Sort-Merge Join (default for OS Spark)

**Bucketing**
- “If you are bucketing datasets, you are doing it wrong” - DT
- Bucketing is hard to get right and is an expensive operation to being with… especially if you are bucketing a periodically changing dataset
- Eliminates the sort in the Sort-Merge Join by pre-sorting partitions
- The cost is paid in production of the dataset on the assumption that savings will be made by frequent joins of both tables
- Not worth considering for datasets less than 1-5 TBs
- DT = Daniel Tomes, from one of his presentations are Spark Summit

</details>



<details>
<summary>3. Spill</summary>

### Problem
- Spill is the term used to refer to the act of moving data from RAM to disk, and later back into RAM again
- This occurs when a given partition is simply too large to fit into RAM
- In this case, Spark is forced into **potentially** expensive disk reads and writes to free up local RAM
- All of this just to avoid the dreaded OOM Error


### Examples
- Set **spark.sql.files.maxPartitionBytes** too high (default is 128 MB)
- The **explode**() of even a small array
- The **join**() or **crossJoin**() of two tables which generates lots of new rows
- The **join**() or **crossJoin**() of two tables by a skewed key
- The **groupBy**() where the column has low cardinality
- The **countDistinct**() and **size(collect_set)**()
- Setting **spark.sql.shuffle.partitions** too low or wrong use of **repartition**()

### Spill Types
In the Spark UI, spill is represented by two values:
1. **Spill (Memory)**: For the partition that was spilled, this is the size of that data as it existed in memory
2. **Spill (Disk)**: Likewise, for the partition that was spilled, this is the size of the data as it existed on disk

### Solution
- Allocate cluster with more RAM per Core
- Address data skew
- Manage size of Spark partitions
- Avoid expensive operations like **explode**()
- Reduce amount data preemptively whenever possible 
- Analyze Join Columns -> sql("ANALYZE TABLE transactions COMPUTE STATISTICS FOR COLUMNS country_id, store_id")
- Set **spark.sql.shuffle.partitions** = auto

</details>

<details>
<summary>4. Serialization</summary>

###  Problem

- Spark SQL and DataFrame instructions are highly optimized
- All UDFs must be serialized and distributed to each executor
- The parameters and return value of each UDF must be converted for each row of data before distributing to executors
- Python UDFs takes an even harder hit
- - The Python code has to be pickled
- - Spark must instantiate a Python interpreter in each and every Executor
- - The conversion of each row from Python to DataFrame costs even more
- UDFs create an analysis barrier for the Catalyst Optimizer
- The Catalyst Optimizer cannot connect code before and after UDF
- The UDF is a black box which means optimizations are limited to the code before and after, excluding the UDF and how all the code works together

### Solution
- Don’t use UDFs
- - I challenge you to find a set of transformations that cannot be done with the built-in, continuously optimized, community supported, higher-order functions
- If you have to use UDFs in Python (common for Data Scientist) use the Vectorized UDFs as opposed to the stock Python UDFs or Apache Arrow Optimised Python UDFs
- If you have to use UDFs in Scala use Typed Transformations as opposed to the stock Scala UDFs
- Resist the temptation to use UDFs to integrate Spark code with existing business logic - porting that logic to Spark almost always pays off

</details>

<hr/>

### Choosing Cluster Type

1. All Purpose
- Designed to handle interactive workloads, including streaming workloads.
- Enable Auto-Scale to add capacity when needed and reduce time to answer
- Security considerations must be considered as auto-scaling can introduce additional risks.
2. Job Purpose
- Run on ephemeral clusters that are created for the job, and terminate on completion
- Pre-scheduled or submitted via API
- Single-user
- Great for isolation and debugging
- Production and repeat workloads
- Lower cost

3. SQL Warehouse
- Built for high concurrency ad-hoc SQL analytics and BI serving
- Photon included 
- Recommended shared warehouse for ad-hoc SQL analytics, isolated warehouse for specific workloads
- Serverless available for instant startup and lower TCO


### Photon

- Save on compute costs (  saving up to 40%  )
- Fast query performance ( Built for modern hardware with up to 12x better price/perf compared to other cloud data warehouses )
- No code changes ( Spark APIs that can do exploration, ETL, big data, small data, low latency, high concurrency, batch, and streaming )
- Broad language support ( Support for SQL, Python, Scala, R, and Java )

### Cluster Optimization Recommendations

1. DS & DE development: all-purpose compute, auto-scale and auto-stop enabled, develop & test on a subset of the data
2. Ingestion & ETL jobs: jobs compute, size accordingly to job SLA
3. Ad-hoc SQL analytics: (serverless) SQL warehouse, auto-scale and auto-stop enabled
4. BI Reporting: isolated SQL warehouse, sized according to BI SLAs
5. Best practices:
- a. Enable spot instances on worker nodes
- b. Use the latest LTS Databricks Runtime when possible
- c. Use Photon for best TCO when applicable
- d. Use latest gen VM, start with general purpose, then test memory/compute optimized


### Selection Instance Type

1. **AWS**
- i3’s aren't always the best. Explore m7gd and r7gd Enable caching if needed.
- Graviton instances work well, try those first ○ M7gd and r7gd have better processors, similar (albeit smaller) local disk and much more stable spot markets than i-series
2. **Azure**
- try the eav4, dav4 and f-series over L-series ○ The ACU is very useful
3. **GCP**
- defaults are pretty good
- Usually don’t need network optimized instance types some occasions they help with Photon

#### Setup recommendation

Rules of thumb
- First run: set spark.sql.shuffle.partitions = 2x # of cores ○ Keep total memory available to the machine less than 128gb
- Number of cores should be a ratio of 1 core to 128mb -> 2gb of reads (Some caveats may apply)
- Avoid setting any other configs at first (don’t carry over configs from legacy platforms unless absolutely necessary)

### Sizing a Driver

- Leave it the same size as your worker unless you care about being the absolute cheapest - dont make things more complicated than they need to be.
- Driver typically do very little work in a Spark application. Using a 4-8 core 16-32gb ram driver should be fine for most workloads
- Large commits to delta tables use more memory.
- This suggestion is voided when: ○ Running many streams/concurrent jobs on the same machine ○ Commiting a very large (100k+ files) amount of data to a delta table ○ Collecting large amount of data to the driver to use in Pandas/R

