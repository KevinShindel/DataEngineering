from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, LongType, StringType, StringType, StructType, IntegerType

# df_schema = [StructField(name='Id', dataType=LongType(), nullable=False)]
# df_struc = StructType(fields=df_schema)

schema = StructType(fields=[StructField(name='id', dataType=IntegerType()),])



if __name__ == '__main__':
    """
    Spark provides union() method in Dataset class to concatenate or append a Dataset to another.
    To append or concatenate two Datasets use Dataset.union() method on the first dataset and provide second Dataset as argument.
    Note: Dataset Union can only be performed on Datasets with the same number of columns.
    """
    spark = SparkSession.builder.appName(__name__).master("local[2]").getOrCreate()

    df1 = spark.createDataFrame(data=[[1],[2],[3],[4]], schema=['id'])
    df2 = spark.createDataFrame(data=[[5],[6],[7],[8]], schema=['id'])
    df1.show()
    df2.show()

    df3 = df1.union(df2)
    df3.show()


    data = [["java", "dbms", "python"],
            ["OOPS", "SQL", "Machine Learning"]]

    # giving column names of dataframe
    columns = ["Subject 1", "Subject 2", "Subject 3"]

    # creating a dataframe
    dataframe = spark.createDataFrame(data, columns)
    dataframe.show()


