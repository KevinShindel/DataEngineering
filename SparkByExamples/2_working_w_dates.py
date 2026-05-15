import time

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, lit, to_date

if __name__ == '__main__':
    conf = SparkConf().setAll([('spark.executor.memory', '8g'),
                               ('spark.driver.memory', '8g')])
    spark = SparkSession.builder. \
        appName(__name__). \
        master("local[*]"). \
        config(conf=conf). \
        getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    df = spark.createDataFrame([('2019-12-25 13:39:00',)], ['X-mas'])
    df.show()

    df.select(
        to_date(lit('2019-12-25')),
        to_date(lit('2019-25-15')),
    ).show(1)

    df.printSchema()

    df.select(to_date(col('X-mas'), 'yyyy-MM-dd HH:mm:ss').alias('Date Format'),
              to_timestamp(col('X-mas'), 'yyyy-MM-dd HH:mm:ss').alias('TimeStampFormat')).\
        show(1)

    df.select(to_date(lit('25/Dec/2019 13:30:00'), 'dd/MMM/yyyy HH:mm:ss').
              alias('X-mas')).\
        show(1)

    df.select(to_date(col('X-mas'), 'MM/dd/yyyy hh:mm:ss a'),
              to_timestamp(col('X-mas'), 'MM/dd/yyyy hh:mm:ss a')).\
        show(1, truncate=False)

    spark.stop()





