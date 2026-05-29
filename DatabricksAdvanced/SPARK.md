

- Available CPU cores = spark.conf.get("spark.executor.cores")
- Memory Resources = spark.conf.get("spark.executor.memory")

JOB → Stages → Task

Job Execution Plan

Analysis → Logical Optimization → Physical Optimization → Code Generation

- Catalyst Responsible for:
- - Query optimization engine
- - Applies rules-based and cost-based optimizations
- - Improve query performance
- - Catalyst transforms DataFrame operations into optimized physical plans by applying rule-based optimizations like predicate pushdown and column pruning

- Photon Engine Responsible for:
- - Databricks-native vectorized query engine for accelerating query execution
- - Processed data in batches rather that row-by-row
- - Runs by default on Databricks SQL warehouses and serverless compute
- - Can be enabled on All-Purpose and Job Clusters

### DataFrame Schema Definition

- DDL
```python
ddl_schema = "name STRING NOT NULL, age INT, city STRING"
df = spark.read.csv(path,schema=ddl_schema)
```
- StructType
```python
from pyspark.sql import types as T
schema = T.StructType([
    T.StructType("customerId", T.IntegerType(), False),
    T.StructField("name", T.StringType(), True),
    T.StructField("age", T.IntegerType(), True),
])

df = spark.read.csv(path, schema=schema)
```

| DataType   | SQL TYPE | Python-Based | Notes            | 
|------------|----------|--------------|------------------|
| ArrayType  | ARRAY    | list         | Ordered elements |
| MapType    | MAP      | dict         | Keys and values  |
| StructType | STRUCT   | typle, dict  | Named Fields     |


### Working with NA

| Operation     | Example                             | Notes | 
|---------------|-------------------------------------|-------|
| isNull        | F.col('col_name').isNull()          |       |
| isNotNull     | F.col('col_name').isNotNull()       |       |
| fillna / fill | df.na.fill('unknown') / df.fillna() |       |
| dropna / drop | df.na.drop() / df.dropna()          |       | 

```python
cleaned_df = df.na.drop(
    how='any',
    subset=['ColumnName']
)
```


### UDF

- by decorator

```python

@udf("string")
def make_greeting(name):
    return f'Hello {name}!'

df.select(make_greeting(F.col('name')))
```

- by function wrapper

```sql

udf_func = udf(my_func)

sales_df = sales_df.withColumn('processed_col', udf_func(F.col('column_name')))
```

- by pandas udf
```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# define pandas udf
@pandas_udf('string')
def pandas_vectorized_udf(row: pd.Series):
    return row.str[0]

# alternative
def pandas_vectorized_udf(email):
    return email.str[0]

vectorized_udf = pandas_udf(pandas_vectorized_udf, 'string')
```