import time

from pyspark import SparkConf
from pyspark.sql import SparkSession

if __name__ == '__main__':
    start = time.perf_counter()
    conf = SparkConf().setAll([('spark.executor.memory', '8g'),
                               ('spark.driver.memory', '8g')])
    spark = SparkSession.builder. \
        appName(__name__). \
        master("local[*]"). \
        config(conf=conf). \
        getOrCreate()

    police = "Exercise Files/police_stations.csv"
    spark.sparkContext.setLogLevel('ERROR')
    sc = spark.sparkContext

    p_rdd = sc.textFile(police)
    ps_header = p_rdd.first()
    ps_rest = p_rdd.filter(lambda line: line != ps_header)

    collected = ps_rest.map(lambda line: line.split(',')).collect()
    counted = ps_rest.map(lambda line: line.split(',')).count()

    ps_rest.filter(lambda line: line.split(',')[0] == '7')

    data = ps_rest.filter(lambda line: line.split(',')[0] == '7'). \
        map(lambda line: (line.split(',')[0],
                          line.split(',')[1],
                          line.split(',')[2],
                          line.split(',')[5],
                          )).collect()
    print(data)
    result = ps_rest. \
        filter(lambda line: line.split(',')[0] in ['10', '11']). \
        map(lambda line: (
        line.split(',')[1],
        line.split(',')[2],
        line.split(',')[5],
    )
            ).collect()
    print(result)
