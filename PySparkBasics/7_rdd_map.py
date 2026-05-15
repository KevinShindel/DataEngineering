from functools import reduce

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    """
    Spark RDD map()
    In this Spark Tutorial, we shall learn to map one RDD to another.
     Mapping is transforming each RDD element using a function and returning a new RDD. 
     Simple example would be calculating logarithmic value of each RDD element (RDD<Integer>) 
     and creating a new RDD with the returned elements.
    """
    conf = SparkConf().\
        setAppName(__name__).\
        setMaster("local[2]").\
        set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize(range(100))
    result = rdd.map(lambda x: x**2).collect()
    print(result)

