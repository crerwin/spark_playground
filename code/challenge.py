from pyspark import SparkConf, SparkContext
import sys

APP_NAME = "IBK Coding Challenge2"


def main(sc, filename):
    # Spark uses lazy evaluation.  Each transformation describes what WILL
    # happen once we perform an action on the RDD.  None of this executes until
    # the line that assigns longestword.

    # Here we're simply indicating that we want words to eventually be
    # a Spark Resilient Distributed Dataset (RDD).  Once we hit an action,
    # words will be an RDD containing each line (and each line is one word).
    words = sc.textFile(filename)

    # We'll then filter the RDD, resulting in an RDD of items that
    # satisfy the valid_word function
    filteredwords = words.filter(valid_word)

    # Then we'll sort the RDD by the length of the word, descending
    sortedwords = filteredwords.sortBy(lambda word: len(word), ascending=False)

    # None of that has actually happened yet though.  When we call the action
    # .first(), it finally evaluates the contents of the file through all of
    # those defined transformations.
    longestword = sortedwords.first()
    print(longestword)


def main_chained(sc, filename):
    # you can always chain the calls together if you're _that_ type of person.
    print(sc.textFile(filename)
            .filter(valid_word)
            .sortBy(lambda word: len(word), ascending=False)
            .first())


def valid_word(word):
    for letter in list(word):
        if word.lower().count(letter) > 1:
            return False
    return True


if __name__ == "__main__":
    conf = SparkConf().setAppName(APP_NAME)
    sc = SparkContext(conf=conf)
    filename = sys.argv[1]
    main(sc, filename)
