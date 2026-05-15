import os

from pyspark import SparkConf, SparkContext


def absoluteFilePaths(directory):
    for dir_path, _, filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dir_path, f))


if __name__ == '__main__':
    """
    Read Multiple Text Files to Single RDD
    In this example, we have three text files to read. 
    We take the file paths of these three files as comma separated valued in a single string literal. 
    Then using textFile() method, we can read the content of all these three text files into a single RDD.
    """
    files = ','.join(list(absoluteFilePaths('data')))
    print(files)
    conf = SparkConf().setAppName("Read Multiple Text Files to RDD").setMaster("local[2]").set("spark.executor.memory","2g")
    sc = SparkContext(conf=conf)
    # rdd = sc.textFile(files)
    # rdd.foreach(lambda x: print(x))
    # data = rdd.collect()
    # print(data)

    dirs = ','.join([
        os.path.join(os.path.abspath(os.path.curdir), 'data/'),
        os.path.join(os.path.abspath(os.path.curdir), 'example/')
    ])
    print(dirs)

    # read list of dirs -> repartition to 2, and save
    rdd = sc.textFile(dirs).\
        repartition(2).\
        saveAsTextFile('result')
