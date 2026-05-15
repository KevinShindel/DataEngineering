from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    """
    To get distinct elements of an RDD, apply the function distinct on the RDD.
     The method returns an RDD containing unique/distinct elements.
    """
    conf = SparkConf().\
        setAppName(__name__).\
        setMaster("local[2]").\
        set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize(range(100))
    result = rdd.reduce(lambda a,b:a+b)
    print(result)

