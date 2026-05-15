from pyspark.sql import SparkSession
from pyspark.sql.functions import first


def main():
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

    df = spark.createDataFrame([], "measure_name string, value int")

    study_metrics_df = (df.select('measure_name', 'value').
                        groupBy().
                        pivot('measure_name').
                        agg(first('value')))

    study_metrics_df.show()


if __name__ == '__main__':
    main()
