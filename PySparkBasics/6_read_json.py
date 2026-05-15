from pyspark.sql.session import SparkSession

if __name__ == '__main__':
    """
    Steps to Read JSON file to Spark RDD
    To read JSON file Spark RDD,
    """
    spark = SparkSession.builder.appName(__name__).master("local[2]").getOrCreate()
    data = spark.read.json('example.json')
    rdd = data.rdd
    rdd.foreach(lambda x: print(x))
