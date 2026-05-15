from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

if __name__ == '__main__':
    """
    In the following example, we shall add a new column with name “new_col” with a constant value.
     We shall use functions.lit(Object literal) to create a new Column.
    """
    spark = SparkSession.builder.appName(__name__).master("local[2]").getOrCreate()
    df = spark.read.format('json').load('example.json')
    df.show()
    newDs = df.withColumn("new_col", lit(1))  # add new column with static 1 value
    newDs.show()
    spark.stop()

