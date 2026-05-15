from pyspark.sql import SparkSession

if __name__ == '__main__':
    """
   Dataset class provides an interface for saving the content of the non-streaming Dataset out into external storage.
    JSON is one of the many formats it provides. In this tutorial, we shall learn to write Dataset to a JSON file.
    """
    spark = SparkSession.builder.appName(__name__).master("local[2]").getOrCreate()
    df = spark.read.format('json').load('example.json')
    df.show()
    df.write.format('json').save('result')
