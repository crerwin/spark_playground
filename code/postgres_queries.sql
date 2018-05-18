-- dumping ground for copy-paste queries


-- trip_count
SELECT COUNT(*) AS trip_count FROM taxi;

-- list_vendor_ids
SELECT DISTINCT vendorid FROM taxi;

-- total_passengers
SELECT SUM(passenger_count) FROM taxi;

-- average_passenger_count
SELECT AVG(passenger_count) AS FROM taxi;

-- slow_join
SELECT t.vendorid, count(*) FROM taxi t
INNER JOIN
  (SELECT vendorid
  FROM taxi WHERE trip_distance > 5
  GROUP BY vendorid
  ORDER BY SUM(passenger_count) ASC
  LIMIT 1) t2
on t.vendorid = t2.vendorid
GROUP BY t.vendorid;

SELECT t.dolocationid
FROM taxi t
JOIN taxi_zone_lookup z ON t.dolocationid = z.locationid
WHERE z.borough = 'Brooklyn' AND
  (date_part('hour', t.tpep_dropoff_datetime) > 18 OR
    date_part('hour', t.tpep_dropoff_datetime) < 6)
GROUP BY t.dolocationid, z.borough, z.zone
ORDER BY SUM(passenger_count) DESC
LIMIT 1;

SELECT SUM(tip_amount) AS tip_sum,
SUM(total_amount) AS revenue,
(SUM(tip_amount)/(SUM(total_amount)-SUM(tip_amount))) AS tip_percentage
FROM taxi t
WHERE t.payment_type = 1 AND t.vendorid = 1 AND t.dolocationid = (
  SELECT t.dolocationid
  FROM taxi t
  JOIN taxi_zone_lookup z ON t.dolocationid = z.locationid
  WHERE z.borough = 'Brooklyn' AND
    (date_part('hour', t.tpep_dropoff_datetime) > 18 OR
      date_part('hour', t.tpep_dropoff_datetime) < 6)
  GROUP BY t.dolocationid, z.borough, z.zone
  ORDER BY SUM(passenger_count) DESC
  LIMIT 1
) AND (date_part('hour', t.tpep_dropoff_datetime) > 18 OR
  date_part('hour', t.tpep_dropoff_datetime) < 6);

SELECT count(*) as rides, SUM(tip_amount) AS tip_sum,
SUM(total_amount) AS revenue,
(SUM(tip_amount)/(SUM(total_amount)-SUM(tip_amount))) AS tip_percentage
FROM taxi t
WHERE t.payment_type = 1 AND t.vendorid = 1 AND t.dolocationid = 181 AND
(date_part('hour', t.tpep_dropoff_datetime) > 18 OR
  date_part('hour', t.tpep_dropoff_datetime) < 6);

SELECT count(*) as rides
FROM taxi t
JOIN taxi_zone_lookup z ON t.dolocationid = z.locationid
WHERE t.vendorid = 1 AND z.borough = 'Brooklyn' AND
(date_part('hour', t.tpep_dropoff_datetime) > 18 OR
  date_part('hour', t.tpep_dropoff_datetime) < 6);

  SELECT distinct z.zone
  FROM taxi t
  JOIN taxi_zone_lookup z ON t.dolocationid = z.locationid
  WHERE t.vendorid = 1 AND z.borough = 'Brooklyn' AND
  (date_part('hour', t.tpep_dropoff_datetime) > 18 OR
    date_part('hour', t.tpep_dropoff_datetime) < 6);

(
  SELECT vendorid
  FROM taxi WHERE trip_distance > 5
  GROUP BY vendorid
  ORDER BY SUM(passenger_count) ASC
  LIMIT 1
)

select count(*)
from taxi t
where (date_part('hour', t.tpep_pickup_datetime) < 18 AND
  date_part('hour', t.tpep_dropoff_datetime) > 18);

CREATE TABLE taxi2
  (vendorid SMALLINT, tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP, passenger_count SMALLINT,
  trip_distance FLOAT, ratecodeid SMALLINT, store_and_fwd_flag CHAR(1),
  pulocationid SMALLINT, dolocationid SMALLINT, payment_type SMALLINT,
  fare_amount MONEY, extra MONEY, mta_tax MONEY, tip_amount MONEY,
  tolls_amount MONEY, improvement_surcharge MONEY, total_amount MONEY);

COPY (
SELECT z.borough, t2.* FROM taxi2 t2
LEFT JOIN (
  SELECT *
  FROM taxi t
  JOIN taxi_zone_lookup z ON t.dolocationid = z.locationid
  WHERE t.vendorid = 1 AND z.borough = 'Brooklyn' AND
  (date_part('hour', t.tpep_dropoff_datetime) > 18 OR
    date_part('hour', t.tpep_dropoff_datetime) < 6)
) t1
ON t1.vendorid = t2.vendorid
and t1.tpep_pickup_datetime = t2.tpep_pickup_datetime
and t1.tpep_dropoff_datetime = t2.tpep_dropoff_datetime
and t1.passenger_count = t2.passenger_count
JOIN taxi_zone_lookup z on t2.dolocationid = z.locationid
WHERE t1.vendorid IS NULL
)
to '/tmp/data/join.csv' WITH CSV DELIMITER ',' HEADER;

COPY (
SELECT z.borough, t2.* FROM taxi2 t2
RIGHT JOIN (
  SELECT *
  FROM taxi t
  JOIN taxi_zone_lookup z ON t.dolocationid = z.locationid
  WHERE t.vendorid = 1 AND z.borough = 'Brooklyn' AND
  (date_part('hour', t.tpep_dropoff_datetime) > 18 OR
    date_part('hour', t.tpep_dropoff_datetime) < 6)
) t1
ON t1.vendorid = t2.vendorid
and t1.tpep_pickup_datetime = t2.tpep_pickup_datetime
and t1.tpep_dropoff_datetime = t2.tpep_dropoff_datetime
and t1.passenger_count = t2.passenger_count
JOIN taxi_zone_lookup z on t2.dolocationid = z.locationid
WHERE t2.vendorid IS NULL
)
to '/tmp/data/join2.csv' WITH CSV DELIMITER ',' HEADER;
