from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    """
    RDD.foreach() – Print RDD – Python Example
    In the following example, we will write a Java program, 
    where we load RDD from a text file,
     and print the contents of RDD to console using RDD.foreach().
    """
    conf = SparkConf().setMaster('local')
    sc = SparkContext(conf=conf)
    rdd = sc.textFile("t8.shakespeare.txt")
    rdd.foreach(f=lambda x: print(x))
