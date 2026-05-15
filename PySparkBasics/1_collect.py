from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    """
    In the following example, we will write a Python program,
     where we load RDD from a text file,
     and print the contents of RDD to console using RDD.collect().
    """

    # create Spark context with Spark configuration
    # read input text file to RDD
    conf = SparkConf().setMaster('local')
    sc = SparkContext(conf=conf)
    rdd = sc.textFile("t8.shakespeare.txt")

    # collect the RDD to a list
    list_elements = rdd.collect()

    # print the list
    for element in list_elements:
        print(element)
