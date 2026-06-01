## Recommended literature

- [DataFrame — PySpark 4.0.0 documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
- [Column — PySpark 4.0.0 documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html)
- [Functions — PySpark 4.0.0 documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Grouping — PySpark 4.0.0 documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html)



| Function                 | Purpose                              | Example                    |
|--------------------------|--------------------------------------|----------------------------|
| array_contains(col, val) | True/False                           | array_contains(items, 'a') | 
| size(col)                | Return number of elements            | size(items)                | 
| element_at(col, n)       | Return required element              | element_at(items, 2)       | 
| array_distinct(col)      | Removes duplicates in array          | array_distinct(items)      |
| collect_list(col)        | Agg.function that gathers all values | collect_list('product')    |
| collect_set(col)         | Do the same but without duplicates   | collect_set('product')     |
| explode(col)             | Unest the array                      | explode(items)             | 

### Complex Data 

```python
from pyspark.sql import functions as F

events_df = spark.table('kafka_stream_table').select(
    F.col('key').cast('string'),
    F.col('value').cast('string')
)

# nested data
events_df.filter(
    "'value:event_name' = 'finalize'"
).orderBy("key").limit(1)

# manipulate arrays
exploded_df = events_df.withColumn('item', F.explode('items'))

exploded_df.where(F.size('items') > 2)

# collect_set - collect uniq values for a field
# flatten - combines multile arrays into one single
# array_distinct - remove duplicates elements from array

(exploded_df.grouBy('user_id')
            .agg(F.collect_set('event_name').alias('event_history'),
                 F.array_distinct(F.flatten('item.item_id')).alias('cart_history')))
```
### convert json into StructType

```python


# id | json data| 
# 1 | {"user_id": 100, "features": {"score": 0.8}}

schema = T.StructType(
    T.StructField('user_id', T.LongType()),
    T.StructField('features', T.MapType(T.StringType(),
                                        T.DoubleType()),
)

df = df.withColumn('parsed', F.from_json(F.col('json_data'), schema))
```

```python
interests_schema = T.ArrayType(T.StringType())
recent_purchases_json = raw_df.select('recent_purchases').limit(1).collect()[0][0]
recent_purchases_schema = F.schema_of_json(F.lit(recent_purchases_json))

raw_df.select(
    F.col('user_id').cast('integer'),
    F.from_json(F.col('interest'), interests_schema).alias('interests'),
    F.from_json(F.col('recent_purchases'), recent_purchases_schema).alias('recent_purchases'),
    F.col('recent_purchases.price').alias('price'),
    F.col('recent_purchases').getFiled('name').alias('name')
)
```

#### Accessing to related fields

```python
df.select(
    F.col('user.name'), # direct access
    F.col('user').getField('age'), # alternative by method
    F.col('user.scores')[0].alias('first_score') # Nested access
)
```

#### Unnesting nested data in arrays

```python

# 1, ['a','b','c']]

df.select(
    'id', F.explode('items').alias('item')
)
```

### GroupBy using expressions

```python
(df.groupBy(F.col('department'). F.year('hire_date')).sum('revenue'))
```

### Combining Multiple Aggregations

```python
(df.groupBy('department')
 .agg(
    F.sum('salary').alias('total_salary'),
    F.avg('age').alias('avg_age')
))

(df.groupBy('department')
 .agg({
    "salary": "sum",
    "age": "avg"
}))
```

### Window Functions

```python
from pyspark.sql.window import Window
from pyspark.sql import functions as F

windowSpec = Window.partitionBy('department').orderBy('salary')

location_stats = (
    df.withColumn('rank', F.rank().over(windowSpec))
   .withColumn('running_total', F.sum('salary').over(windowSpec))
   .withColumn('prev_salary', F.lag().over(windowSpec))
)

win_by_trips = Window.orderBy(F.desc('total_trips'))
win_by_fare = Window.orderBy(F.desc('avg_fare'))

ranked_loc = (
     location_stats
    .withColumn("trips_rank", F.rank().over(win_by_trips))
    .withColumn('fare_rank', F.rank().over(win_by_fare))
    .withColumn('fare_quantile', F.ntile(5).over(win_by_fare)) # divide into 5 groups by fare
)
```

### Pivoting

```python
pivoted_df = (df.groupBy('user_id')
              .pivot('product_name')
              .agg(F.count('product_id').alias('quantity_purchased'))
              .fillna(0))
```