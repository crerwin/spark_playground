from collections import Counter

import sys
import time


def main(filename):
    words = []
    with open(filename) as f:
        for line in f:
            words.extend(line.strip().split(" "))
    words = list(filter(lambda w: len(w) > 0, words))
    wordcount = list(Counter(words).items())
    wordcount = sorted(wordcount, key=lambda wc: wc[1], reverse=True)

    print(wordcount[0:20])


if __name__ == "__main__":
    start_time = time.time()
    filename = sys.argv[1]
    main(filename)
    print("execution time (s): " + str((time.time() - start_time)))
