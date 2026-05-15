from pyspark.sql import SparkSession

if __name__ == '__main__':
    """
    """
    spark = SparkSession.builder.appName(__name__).master("local[*]").getOrCreate()
    sc = spark.sparkContext

    # Narrow Transformation
    # Narrow transformations are the result of map() and filter() functions and these compute data that live on a single partition meaning there will not be any data movement between partitions to execute narrow transformations.
    # Functions such as map(), mapPartition(), flatMap(), filter(), union() are some examples of narrow transformation
    data = range(100)
    rdd = sc.parallelize(data, 6).cache()  # Caches the RDD
    rdd2 = sc.parallelize(range(20), 4).cache()

    # result = rdd.sample() # 	Returns the sample dataset.

    # MAP Functions
    rdd.map(lambda x: x**2)
    rdd.flatMap(lambda x: x**2) # Returns flattern map meaning if you have a dataset with array, it converts each elements in a array as a row. In other words it return 0 or more items in output for each element in dataset.
    rdd.mapPartitions(lambda x: x**2) # Similar to map, but executs transformation function on each partition, This gives better performance than map function
    rdd.mapPartitionsWithIndex(lambda x: x**2) # Similar to map Partitions, but also provides func with an integer value representing the index of the partition

    (res1, res2) = rdd.randomSplit([0.3, 0.7])
    print(res1.count(), res2.count())

    # Filter Example
    result = rdd.filter(lambda x: x % 4 == 0).count()
    print('filter result: ', result)

    # combinations functions
    un = rdd.union(rdd2).count() # Comines elements from source dataset and the argument and returns combined dataset. This is similar to union function in Math set operations.
    print('union result', un)

    intersect = rdd.intersection(rdd2).collect() # Returns the dataset which contains elements in both source dataset and an argument
    print('intersection: ', intersect)

    # Partitions Operations
    result = rdd.repartition(2) # Return a dataset with number of partition specified in the argument. This operation reshuffles the RDD randamly, It could either return lesser or more partioned RDD based on the input supplied.
    print('repartition partitions: ', result.getNumPartitions())

    result = rdd.coalesce(1) #  Similar to repartition by operates better when we want to the decrease the partitions. Betterment acheives by reshuffling the data from fewer nodes compared with all nodes by repartition.
    print('coalesce partition: ', result.getNumPartitions())

    # Sorting Operations
    # rdd.sortBy()
    # rdd.sortByKey() # transformation is used to sort RDD elements on key. In our example, first, we convert RDD[(String,Int]) to RDD[(Int,String]) using map transformation and apply sortByKey which ideally does sort on an integer value. And finally, foreach with println statement prints all words in RDD and their count as key-value pair to console.
    result = rdd.reduce(lambda a,b: a+b) #
    print('reduce: ', result)
    # rdd.reduceByKey() # merges the values for each key with the function specified. In our example, it reduces the word string by applying the sum function on value. The result of our RDD contains unique words and their count.

    # Aggregation Functions
    # rdd.aggregate(3) # TODO:

    # rdd.fold()
    # fold() is similar to reduce() except it takes a ‘Zero value‘ as an initial value for each partition.
    # fold() is similar to aggregate() with a difference; fold return type should be the same as this RDD element type whereas aggregation can return any type.
    # fold() also same as foldByKey() except foldByKey() operates on Pair RDD

    # rdd.foldByKey() #

    # Wider Transformation
    # Wider transformations are the result of groupByKey() and reduceByKey() functions and these compute data that live on many partitions meaning there will be data movements between partitions to execute wider transformations. Since these shuffles the data, they also called shuffle transformations.
    # Functions such as groupByKey(), aggregateByKey(), aggregate(), join(), repartition() are some examples of a wider transformations.






