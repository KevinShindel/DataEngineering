from operator import add

from pyspark import SparkContext, SparkConf

# //////////////////////////////////////////////////////////////////////
# ---------------- EXAMPLE 1--------------------------------------------
# //////////////////////////////////////////////////////////////////////
TEXT_PATH = 't8.shakespeare.txt'


def spark_context():
    conf = SparkConf().setMaster('local')
    return SparkContext(conf=conf)  # create sparkContext


def tokenize(text):
    return text.split()


def main():
    # Example 1 ( read text file )
    # Using SparkContext
    sc = spark_context()
    data = sc.textFile(TEXT_PATH)  # open file
    words = data.flatMap(tokenize)  # split text to arrays
    wc = words.map(lambda x: (x, 1))  # create counter
    counts = wc.reduceByKey(add)  # count words
    counts.saveAsTextFile("wc")  # save result to file


if __name__ == '__main__':
    main()  # spark-submit --master spark://192.168.1.11:4040 --deploy-mode client  main.py
