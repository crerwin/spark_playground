# Playing with Apache Spark

I won't pretend to be qualified to give an overview of Spark.  In fact, I
created this repo so _I_ could play with Spark.  Check out Spark's docs for an
overview, and then feel free to use the resources in this overview to compare
it with pure Python, and with a standard RDMS as appropriate.

## Note on Repo layout
These instructions make use of containers and the `code/` and `data/` folders
get mapped to volumes on the containers.  The instructions include steps for
acquiring the test data.  You don't have to put it inside `data/` but that's
the assumed location.

`conf/` contains Spark configurations used in the `docker-compose.yml` cluster.

## Running Spark
Note: the example commands below use the complete works of Shakespeare text
file, which is not checked into this repo.  See The Word Count section for
instructions on acquiring the file.

You can install Spark locally if you want, but it requires Java (gross).  If you
don't need it locally, you can run everything in this repo in a container.  The
`gettyimages/spark` Docker image works great.

To run a Pyspark application locally you'd use the `spark-submit` command:

```
spark-submit code/wordcount_spark.py data/t8.shakespeare.txt
```

To run it in a container, you'd use the same command within the container:

```
docker run --rm -it -v $(pwd)/code:/tmp/code -v $(pwd)/data:/tmp/data gettyimages/spark bin/spark-submit /tmp/code/wordcount_spark.py /tmp/data/t8.shakespeare.txt
```

To run it in a cluster of containers, you can use docker compose with the
provided `docker-compose.yml` file.  I like to use two terminal windows:

```
docker-compose up
```

This will spin up a master node and a worker node and will show you running
`stdout` and `stderr` from both.  the `code/` and `data/` folders have been
mapped to `/tmp/code` and `/tmp/data` on both the master and worker.  You can
access the admin interface for the cluster at `localhost:8080`

In another terminal window you can again use the `spark-submit` command within
the master container to run the code on the cluster:

```
docker exec -it spark_playground_master_1 bin/spark-submit /tmp/code/wordcount_spark.py /tmp/data/t8.shakespeare.txt
```

You can scale the cluster up if you want (in the second terminal window):

```
docker-compose -d --scale worker=3
```

Note that if you scale the number of workers to be equal to or greater than your
physical cores, you may run into timeouts as the master will have trouble
communicating with the workers.  Each worker's web interface is mapped to an
arbitrary port between `8081` and `8099`.  The links in the master's admin
interface point to `8081` for every worker and will therefore not necessarily
work.  You can get the actual port by checking `docker ps`.


# Example Code

## Word Count

Spark can split a text file into words and count them for you.  So can pure
Python, but Spark is really good at scaling this.

### Sample Data
You can get the complete works of Shakespeare in a text file from `https://ocw.mit.edu/ans7870/6/6.006/s08/lecturenotes/files/t8.shakespeare.txt`

```
wget https://ocw.mit.edu/ans7870/6/6.006/s08/lecturenotes/files/t8.shakespeare.txt
```

Run the code to get the 20 most common words:

```
docker run --rm -it -p 4040:4040 -v $(pwd)/code:/tmp/code -v $(pwd)/data:/tmp/data gettyimages/spark bin/spark-submit /tmp/code/wordcount_spark.py /tmp/data/shakespeare.txt
```

# Taxis

## Getting the data
Download `https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2017-01.csv`
or any other yellow taxi csv from http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml

For ease of typing, let's change the file name:

```
mv yellow_tripdata_2017-01.csv taxi.csv
```

Two fun aspects of these files is that a) they have blank lines on the second line and b) they use \r\n line endings.  Python and Spark don't care (and the example code here has been written to expect them), but if you want to load the data into Postgres it's easiest to create a fixed file (will take a minute or so):

```
sed '/^.$/d' taxi.csv | tr -d '\r' > taxi_fixed_for_postgres.csv
```

Confirm that the new file only has one less line than the old file:

```
wc -l taxi*.csv
```

And that it has no `\r` characters like `taxi.csv`:

```
head -n 10 taxi.csv | od -c
head -n 10 taxi_fixed_for_postgres.csv | od -c
```


Slow join:
```
for the vendor who transported fewer passengers on all trips over 5 miles,
what is the average tip percentage for credit card tips (payment type: 1, tip/total_amount-tip)
for trips where the drop-off occurred between 6pm and 6am in Brooklyn.
```
