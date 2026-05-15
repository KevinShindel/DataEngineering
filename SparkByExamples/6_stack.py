from pyspark.sql.functions import col, mean, expr, round, sum
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("SparkByExamples.com") \
        .getOrCreate()

    df = spark.createDataFrame([], 'a string')

    users_sample = 'users_sample'
    triers_sample = 'triers_sample'
    non_triers_sample = 'non_triers_sample'
    users_ms = 'users_ms'
    triers_ms = 'triers_ms'
    non_triers_ms = 'non_triers_ms'

    expression = (col('U*U') + col('T*T') + col('N*N')) / (
                    col(users_sample) + col(triers_sample) + col(non_triers_sample))

    result_df = df.withColumn('U*U', col(users_sample) * col(users_ms)) \
        .withColumn('T*T', col(triers_sample) * col(triers_ms)) \
        .withColumn('N*N', col(non_triers_sample) * col(non_triers_ms)) \
        .withColumn('Adjusted Persuasion', expression) \
        .groupBy() \
        .agg(
        (sum('U*U') / sum(users_sample)).alias('persuasion_users_mean'),
        (sum('T*T') / sum(triers_sample)).alias('persuasion_trialists_mean'),
        (sum('N*N') / sum(non_triers_sample)).alias('persuasion_non_trialists_mean'),
        mean('Adjusted Persuasion').alias('persuasion_mean')
    ) \
        .select(expr("stack(4,"
                     " 'Persuasion Users Norm', persuasion_users_mean,"
                     " 'Persuasion Trialists Norm', persuasion_trialists_mean,"
                     " 'Persuasion NonTrialists Norm', persuasion_non_trialists_mean,"
                     " 'Persuasion Norm', persuasion_mean"
                     ") as (coefficient, value)")) \
        .select('coefficient', round('value', 9).alias('value'))

    result_df.show()


if __name__ == "__main__":
    main()
