from pyspark.sql import SparkSession

if __name__ == '__main__':
    """
    Load data from JSON data source and execute Spark SQL query
    Apache Spark Dataset and DataFrame APIs provides an abstraction to the Spark SQL 
    from data sources. Dataset provides the goodies of RDDs along with the optimization benefits of Spark SQL’s
     execution engine.
    """
    spark = SparkSession.builder.appName(__name__).master("local[*]").getOrCreate()
    data = spark.read.json("example.json")
    data = data.drop('_corrupt_record').na.drop()
    data.createOrReplaceTempView("people")

    query = "SELECT * FROM people WHERE salary > 3000"
    result = spark.sql(query)
    result.show()
    query = "SELECT * FROM people"
    result = spark.sql(query)
    result.show()
    spark.stop()
