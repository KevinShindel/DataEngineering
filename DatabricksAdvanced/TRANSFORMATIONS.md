
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

exploded_df.grouBy('user_id')
            .agg(F.collect_set('event_name').alias('event_history'),
                 F.array_distinct(F.flatten('item.item_id')).alias('cart_history'))
```