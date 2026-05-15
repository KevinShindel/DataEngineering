import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, lit, date_add, date_sub, min, max
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType, LongType
from pyspark import SparkConf

if __name__ == '__main__':
    start = time.perf_counter()
    conf = SparkConf().setAll([('spark.executor.memory', '8g'),
                               ('spark.driver.memory', '8g')])
    spark = SparkSession.builder.\
        appName(__name__).\
        master("local[*]").\
        config(conf=conf).\
        getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    path = "Exercise Files/reported-crimes.csv"
    DT_FMT = 'MM/dd/yyyy hh:mm:ss a'

    schema = StructType([
        StructField("ID", LongType(), True),
        StructField("Case Number", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Block", StringType(), True),
        StructField("IUCR", IntegerType(), True),
        StructField("Primary Type", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Location Description", StringType(), True),
        StructField("Arrest", BooleanType(), True),
        StructField("Domestic", BooleanType(), True),
        StructField("Beat", IntegerType(), True),
        StructField("District", IntegerType(), True),
        StructField("Ward", IntegerType(), True),
        StructField("Community Area", IntegerType(), True),
        StructField("FBI Code", StringType(), True),
        StructField("X Coordinate", LongType(), True),
        StructField("Y Coordinate", LongType(), True),
        StructField("Year", IntegerType(), True),
        StructField("Updated On", StringType(), True),
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
        StructField("Location", StringType(), True),
        StructField("Historical Wards 2003-2015", IntegerType(), True),
        StructField("Zip Codes", IntegerType(), True),
        StructField("Community Areas", IntegerType(), True),
        StructField("Census Tracts", IntegerType(), True),
        StructField("Wards", IntegerType(), True),
        StructField("Boundaries - ZIP Codes", IntegerType(), True),
        StructField("Police Districts", StringType(), True),
        StructField("Police Beats", IntegerType(), True),
    ])

    df = spark.read.csv(path=path, header=True, schema=schema).\
        withColumn('Date', to_timestamp(col('Date'), DT_FMT)).\
        withColumn('Updated On', to_timestamp(col('Updated On'), DT_FMT)).cache()

    df.printSchema()
    df.select('Date', 'Updated On').show(5)
    print('Crimes before 2019-01-01 --> ', df.filter(col('Date') <= lit('2019-01-01')).count())

    one_day = df.filter(col('Date') == lit('2018-11-12'))
    print('Crimes at 2018-11-12', one_day.count())
    total_arrests = df.filter(col('Arrest') == 'true').count() / df.select('Arrest').count()
    print('Total arrested: ', total_arrests)

    df.groupby('Location Description').count().orderBy('count', ascending=False).show(3) # show 3 most crimes region
    df.select(min(col('Date')), max(col('Date'))).show(1)  # show oldest and most recent date
    df.select(date_add(min('Date'), 3).alias('3 Oldest days'), date_sub(max(col('Date')), 3).alias('3 Most recent days')).show(1)

    spark.stop()
    end = time.perf_counter()
    print('Done at', round(end-start, 2), 'seconds')
