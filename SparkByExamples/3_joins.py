import time

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, dayofweek, date_format
import matplotlib.pyplot as plt

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
    police = "Exercise Files/police_stations.csv"
    DT_FMT = 'MM/dd/yyyy hh:mm:ss a'
    rc = spark.read.csv(path=path, header=True, inferSchema=True).\
        withColumn('Date', to_timestamp(col('Date'), DT_FMT)).\
        withColumn('Updated On', to_timestamp(col('Updated On'), DT_FMT)).\
        cache()

    ps = spark.read.csv(path=police, header=True, inferSchema=True).cache()

    # print('++++++++++++++++POLICE STATIONS++++++++++++++++++++')
    # ps.select(col('DISTRICT')).distinct().show()
    # print('++++++++++++++++REPORTED CRIMES++++++++++++++++++++')
    # rc.select(col('District')).distinct().show(30)

    # ps.select(lpad(col('DISTRICT'), 3, '0')).show() # 1 -> 001 for correct join
    # ps = ps.withColumn('Format_district', lpad(col('DISTRICT'), 3, '0'))

    ps = ps.withColumnRenamed('Format_district', 'District')
    ps.show(5)

    rc = rc.join(other=ps, on='District', how='left_outer')
    rc.show(10, truncate=False)

    # rc.groupby('Location Description').count().orderBy('count', ascending=False).show(3) # show 3 most crimes region
    #
    # rc.select(col('Primary Type')).distinct().orderBy(col('Primary Type')).show(35, truncate=False)

    # Add column IS_CRIMINAL
    # rc = rc.withColumn('IS_CRIMINAL', (col('Primary Type') == 'NON - CRIMINAL') | (col('Primary Type') == 'NON-CRIMINAL') | (
    #             col('Primary Type') == 'NON-CRIMINAL (SUBJECT SPECIFIED)'))

    # nc = rc.filter((col('Primary Type') == 'NON - CRIMINAL') | (col('Primary Type') == 'NON-CRIMINAL') | (col('Primary Type') == 'NON-CRIMINAL (SUBJECT SPECIFIED)')).cache()
    #
    # nc.show(50)

    # nc.groupBy(col('Description')).count().orderBy('count', ascending=False).show()

    rc.select(col('Date'), dayofweek(col('Date')), date_format(col('Date'), 'E')).show(5)
    dow = rc.groupBy(date_format(col('Date'), 'E').alias('Day of week')).count().orderBy('count', ascending=False).cache()

    dow.show()
    df = dow.toPandas()
    df.sort_values('count').plot(kind='bar', color='olive', x='Day of week', y='count')
    plt.xlabel('Day of the week')
    plt.ylabel('No. of reported crimes')
    plt.title('No. of reported crimes per day of the week')
    plt.show()
    spark.stop()
