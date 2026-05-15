from collections import Counter

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql import SparkSession

if __name__ == '__main__':
    """
    KMeans Classification [https://en.wikipedia.org/wiki/K-means_clustering] using 
    spark MLlib in Java – KMeans algorithm is used for classification. 
    Basically, it classifies each given observation/experiment/vector into one of the cluster. 
    That cluster is chosen, whose mean vector is less distant to the observation/experiment/vector.
    """
    spark = SparkSession.builder.appName(__name__).master("local[*]").getOrCreate()
    path = 'example/cc_data.csv'
    data_customer = spark.read.csv(path=path, header=True, inferSchema=True)
    data_customer.printSchema()
    data_customer = data_customer.na.drop()
    # All attributes under consideration are numerical or discrete numeric, hence we need to convert them into
    # features using a Vector Assembler. A vector assembler is a transformer that converts a set of features into
    # a single vector column often referred to as an array of features. Features here are columns. Since customer
    # id is an identifier that won’t be used for clustering, we first extract the required columns using .columns,
    # pass it as an input to Vector Assembler, and then use the transform() to convert the input columns into a
    # single vector column called a feature.
    assemble = VectorAssembler(inputCols=[
        'BALANCE',
        'BALANCE_FREQUENCY',
        'PURCHASES',
        'ONEOFF_PURCHASES',
        'INSTALLMENTS_PURCHASES',
        'CASH_ADVANCE',
        'PURCHASES_FREQUENCY',
        'ONEOFF_PURCHASES_FREQUENCY',
        'PURCHASES_INSTALLMENTS_FREQUENCY',
        'CASH_ADVANCE_FREQUENCY',
        'CASH_ADVANCE_TRX',
        'PURCHASES_TRX',
        'CREDIT_LIMIT',
        'PAYMENTS',
        'MINIMUM_PAYMENTS',
        'PRC_FULL_PAYMENT',
        'TENURE'], outputCol='features')

    assembled_data = assemble.transform(data_customer)
    assembled_data.show(2)

    # Now that all columns are transformed into a single feature vector we need to standardize the data to bring them
    # to a comparable scale. E.g. Balance can have a scale from 10–1000 whereas balance frequency has a scale from
    # 0–1 say. Euclidean distance is always impacted more by variables on a higher scale, hence it’s important
    # to scale the variables out.
    scale = StandardScaler(inputCol='features', outputCol='standardized')
    data_scale = scale.fit(assembled_data)
    data_scale_output = data_scale.transform(assembled_data)
    data_scale_output.show(2)

    # K-means is one of the most commonly used clustering algorithms for grouping data into a predefined number of
    # clusters. The spark.mllib includes a parallelized variant of the k-means++ method called kmeans||.
    # The KMeans function from pyspark.ml.clustering includes the following parameters:
    # k is the number of clusters specified by the user
    # maxIterations is the maximum number of iterations before the clustering algorithm stops. Note that if the
    # intracluster distance doesn’t change beyond the epsilon value mentioned, the iteration will stop irrespective
    # of max iterations initializationMode specifies either random initialization of centroids or initialization
    # via k-means|| (similar to K-means ++) epsilon determines the distance threshold within which k-means is
    # expected to converge initialModel is an optional set of cluster centroids that the user can provide as an input.
    # If this parameter is used, the algorithm just runs once to allocate points to its nearest centroid

    silhouette_score = Counter()
    evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='standardized',
                                    metricName='silhouette', distanceMeasure='squaredEuclidean')
    for i in range(2, 10):
        KMeans_algo = KMeans(featuresCol='standardized', k=i)

        KMeans_fit = KMeans_algo.fit(data_scale_output)

        output = KMeans_fit.transform(data_scale_output)

        score = evaluator.evaluate(output)

        silhouette_score[i] = score

        print("Silhouette Score:", score)

    print(silhouette_score.most_common(1))

    # Visualizing the silhouette scores in a plot
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(1, 1, figsize=(8, 6))
    ax.plot(range(2, 10), silhouette_score.values())
    ax.set_xlabel('k')
    ax.set_ylabel('cost')
    plt.show()




