from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import functions as F
import sys, time

APP_NAME = "Taxi data"

def main(sc, filename, command):
    if command == "total_passengers":
        print("getting total passenger count...")
        total_passengers(sc, filename)
    if command == "average_passenger_count":
        print("getting total passenger count...")
        average_passenger_count(sc, filename)
    else:
        print("no command or invalid command specified")

def total_passengers(sc, filename):
    sqlcontext = SQLContext(sc)
    df = sqlcontext.read.load(filename, format="csv", sep=",", inferSchema="true", header="true")
    totalpassengers = df.select(F.sum(df.passenger_count))
    print(totalpassengers.show())

def average_passenger_count(sc, filename):
    sqlcontext = SQLContext(sc)
    df = sqlcontext.read.load(filename, format="csv", sep=",", inferSchema="true", header="true")
    averagepassengers = df.select(F.avg(df.passenger_count))
    print(averagepassengers.show())


if __name__ == "__main__":
    start_time = time.time()
    conf = SparkConf().setAppName(APP_NAME)
    sc = SparkContext(conf=conf)
    command = sys.argv[1]
    filename = sys.argv[2]
    main(sc, filename, command)
    print("execution time (s): " + str((time.time() - start_time)))
