from pyspark.ml.clustering import LDA
from pyspark.sql import SparkSession

if __name__ == '__main__':
    """
    """
    spark = SparkSession.builder.appName(__name__).master("local[*]").getOrCreate()
    sc = spark.sparkContext

    # Load and parse the data file, converting it to a DataFrame.
    path = "/home/username/Projects/MilitaryLocationMap/LearnApacheSpark/lib/python3.9/site-packages/pyspark/data/mllib/sample_lda_libsvm_data.txt"
    #  Load training data
    dataset = spark.read.format("libsvm").load(path)

    # Trains a LDA model.
    lda = LDA(k=10, maxIter=10)
    model = lda.fit(dataset)

    ll = model.logLikelihood(dataset)
    lp = model.logPerplexity(dataset)
    print("The lower bound on the log likelihood of the entire corpus: " + str(ll))
    print("The upper bound on perplexity: " + str(lp))

    # Describe topics.
    topics = model.describeTopics(3)
    print("The topics described by their top-weighted terms:")
    topics.show(truncate=False)

    # Shows the result
    transformed = model.transform(dataset)
    transformed.show(truncate=False)