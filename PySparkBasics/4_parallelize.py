from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    """
    Spark parallelize() method creates N number of partitions if N is specified, 
    else Spark would set N based on the Spark Cluster the driver program is running on.
    """
    conf = SparkConf().setMaster('local')
    sc = SparkContext(conf=conf)
    data = range(100) # create data from iterator
    rdd = sc.parallelize(data, 4)  # parallelize 100 / 4 partitions
    print(rdd.getNumPartitions()) # get number of partitions
    rdd.saveAsTextFile('data') # save partitions to directory

