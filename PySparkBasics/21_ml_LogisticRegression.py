from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession

if __name__ == '__main__':
    """
    Classification is a task of identifying the features of an entity and classifying the entity to one of the 
    predefined classes/categories.
    Logistic Regression is a model which knows about relation between categorical variable and its corresponding features
    of an experiment.
    Logistic meaning detailed organization and implementation of a complex operation. Which means identifying common 
    features for all examples/experiments and transforming all of the examples to feature vectors.
    """
    spark = SparkSession.builder.appName(__name__).master("local[*]").getOrCreate()
    sc = spark.sparkContext

    # Load and parse the data file, converting it to a DataFrame.
    path = "/home/username/Projects/MilitaryLocationMap/LearnApacheSpark/lib/python3.9/site-packages/pyspark/data/mllib/sample_libsvm_data.txt"
    #  Load training data
    training = spark.read.format("libsvm").load(path)

    lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Fit the model
    lrModel = lr.fit(training)

    # Print the coefficients and intercept for logistic regression
    print("Coefficients: " + str(lrModel.coefficients))
    print("Intercept: " + str(lrModel.intercept))

    # We can also use the multinomial family for binary classification
    mlr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, family="multinomial")

    # Fit the model
    mlrModel = mlr.fit(training)

    # Print the coefficients and intercepts for logistic regression with multinomial family
    print("Multinomial coefficients: " + str(mlrModel.coefficientMatrix))
    print("Multinomial intercepts: " + str(mlrModel.interceptVector))