from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession

if __name__ == '__main__':
    """
    Apache Spark MLlib – Classification using Naive Bayes
    Classification is a task of identifying the features of an entity and classifying the entity to one of the 
    predefined classes/categories based on the previous knowledge.
    Naive Bayes is one of the simplest methods used for classification. Naive Bayes Classifier could be built in 
    scenarios where problem instances (/ examples / data set / training data) could be represented as feature vectors.
     And the distinctive feature of Naive Bayes is : it considers that features independently play a part in deciding 
     the category of the problem instance i.e., Naive Bayes does not care about the correlation between features if
      present any. Despite the fact that many other classifiers beat out Naive Bayes, it is still sustaining in the
       machine learning community because it requires relatively small number of training data for estimating the
        parameters required for classification.
    """
    spark = SparkSession.builder.appName(__name__).master("local[*]").getOrCreate()
    sc = spark.sparkContext

    # Load and parse the data file, converting it to a DataFrame.
    path = "/home/username/Projects/MilitaryLocationMap/LearnApacheSpark/lib/python3.9/site-packages/pyspark/data/mllib/sample_libsvm_data.txt"
    #  Load training data
    data = spark.read.format("libsvm").load(path)

    # Split the data into train and test
    (train, test) = data.randomSplit([0.6, 0.4], 1234)

    # create the trainer and set its parameters
    nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

    # train the model
    model = nb.fit(train)

    # select example rows to display.
    predictions = model.transform(test)
    predictions.show()

    # compute accuracy on the test set
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                                  metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Test set accuracy = " + str(accuracy))
