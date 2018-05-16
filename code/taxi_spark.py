from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import functions as F
import sys
import time

APP_NAME = "Taxi data"


def main(sc, filename, command):
    if command == "total_passengers":
        print("getting total passenger count...")
        total_passengers(sc, filename)
    elif command == "average_passenger_count":
        print("getting total passenger count...")
        average_passenger_count(sc, filename)
    elif command == "trip_count":
        print("getting total trip count...")
        trip_count(sc, filename)
    elif command == "list_vendor_ids":
        print("listing vendor ids...")
        list_vendor_ids(sc, filename)
    else:
        print("no command or invalid command specified")


def list_vendor_ids(sc, filename):
    sqlcontext = SQLContext(sc)
    df = sqlcontext.read.load(filename, format="csv", sep=",", inferSchema="true", header="true")
    vendorids = df.select(df.VendorID).distinct()
    print(vendorids.show())


def trip_count(sc, filename):
    sqlcontext = SQLContext(sc)
    df = sqlcontext.read.load(filename, format="csv", sep=",", inferSchema="true", header="true")
    tripcount = df.select(df.VendorID).count()
    print(tripcount)


def total_passengers(sc, filename):
    sqlcontext = SQLContext(sc)
    df = sqlcontext.read.load(filename, format="csv", sep=",",inferSchema="true", header="true")
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
