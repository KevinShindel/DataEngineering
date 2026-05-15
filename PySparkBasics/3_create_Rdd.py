from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    """
    In this example, we will take a List of strings, and then create a Spark RDD from this list.
    """
    conf = SparkConf().setMaster('local')
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize(range(100))
    data = rdd.collect()
    print(data)
