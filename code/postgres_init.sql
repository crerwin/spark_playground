\echo 'Creating the taxi_data database...'

CREATE DATABASE taxi_data OWNER postgres;

\connect taxi_data

\echo 'Creating taxi_zone_lookup table...'

CREATE TABLE taxi_zone_lookup
  (locationid SMALLINT, borough VARCHAR(50), zone VARCHAR(50),
  service_zone VARCHAR(50));

\echo 'Loading taxi_zone_lookup data from file...'

COPY taxi_zone_lookup FROM '/tmp/data/taxi_zone_lookup.csv' WITH csv header;

\echo 'Creating taxi table...'

CREATE TABLE taxi
  (vendorid SMALLINT, tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP, passenger_count SMALLINT,
  trip_distance FLOAT, ratecodeid SMALLINT, store_and_fwd_flag CHAR(1),
  pulocationid SMALLINT, dolocationid SMALLINT, payment_type SMALLINT,
  fare_amount MONEY, extra MONEY, mta_tax MONEY, tip_amount MONEY,
  tolls_amount MONEY, improvement_surcharge MONEY, total_amount MONEY);

\echo 'Loading taxi data from file...'

COPY taxi FROM '/tmp/data/taxi_fixed_for_postgres.csv' WITH csv header;

\echo 'Initialization complete.'
