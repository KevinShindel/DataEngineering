from pyspark.sql import SparkSession

if __name__ == '__main__':
    """
    park Dataset is the latest API, after RDD and DataFrame, from Spark to work with data. In this tutorial, 
    we shall learn how to read JSON file to Spark Dataset with an example.
    In this tutorial, we will learn how to read a JSON file to Spark Dataset,
     with the help of example Spark Application.
    """
    spark = SparkSession.builder.appName(__name__).master("local[2]").getOrCreate()
    df = spark.read.format('json').load('example.json')
    df.show()
