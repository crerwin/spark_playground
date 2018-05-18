from pyspark import SparkConf, SparkContext
import sys
import time

APP_NAME = "Word Count: "


def main(sc, filename):
    f = sc.textFile(filename)
    counts_rdd = f.flatMap(lambda line: line.split(" ")) \
        .filter(lambda w: len(w) > 0) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda x: (x[1], x[0])) \
        .sortByKey(False)
    word_counts = counts_rdd.collect()
    print(word_counts[0:20])


if __name__ == "__main__":
    start_time = time.time()
    filename = sys.argv[1]
    conf = SparkConf().setAppName(APP_NAME + filename)
    sc = SparkContext(conf=conf)
    main(sc, filename)
    print("execution time (s): " + str((time.time() - start_time)))
