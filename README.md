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
