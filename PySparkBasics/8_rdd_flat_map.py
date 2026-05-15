from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    """
    In this Spark Tutorial, we shall learn to flatMap one RDD to another.
     Flat-Mapping is transforming each RDD element using a function that could return multiple elements to new RDD. 
     Simple example would be applying a flatMap to Strings and using split function to return words to new RDD.
    """
    conf = SparkConf().\
        setAppName(__name__).\
        setMaster("local[2]").\
        set("spark.executor.memory", "2g")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile('./data/text.txt')
    result = rdd.flatMap(lambda x: x.split()).collect()
    print(result)

