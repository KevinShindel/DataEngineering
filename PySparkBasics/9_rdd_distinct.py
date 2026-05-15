from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    """
    Spark RDD Filter : RDD.filter() method returns an RDD with those elements which pass a filter condition (function)
     that is given as argument to the method.
     In this tutorial, we learn to filter RDD containing Integers, and an RDD containing Tuples, with example programs.
    """
    conf = SparkConf().\
        setAppName(__name__).\
        setMaster("local[2]").\
        set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)
    filter_val = lambda x: x % 10 == 0
    rdd = sc.parallelize(range(100))
    result = rdd.filter(filter_val).collect()
    print(result)

