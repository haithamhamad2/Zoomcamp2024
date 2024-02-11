
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `sonic-airfoil-411903.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-sonic-airfoil-411903/green_data_2022.parquet']
);
-- Creating external table referring to gcs path with correct timestamp
CREATE OR REPLACE EXTERNAL TABLE `sonic-airfoil-411903.ny_taxi.external_green_tripdata2`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-sonic-airfoil-411903/c6c9dbb426a644778e671ac90c40a118-0.parquet']
);

SELECT count(1) FROM `sonic-airfoil-411903.ny_taxi.external_green_tripdata`



-- Check green trip data
SELECT * FROM sonic-airfoil-411903.ny_taxi.external_green_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE sonic-airfoil-411903.ny_taxi.green_tripdata_non_partitoned AS
SELECT * FROM sonic-airfoil-411903.ny_taxi.external_green_tripdata2;

select count(distinct(PULocationID)) from `sonic-airfoil-411903.ny_taxi.external_green_tripdata`
0 

select count(distinct(PULocationID)) from `sonic-airfoil-411903.ny_taxi.green_tripdata_non_partitoned`
6.41 MB
select count(1) from `sonic-airfoil-411903.ny_taxi.external_green_tripdata`
where fare_amount = 0
1622



SELECT count(1) FROM `sonic-airfoil-411903.ny_taxi.external_green_tripdata2`


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE sonic-airfoil-411903.ny_taxi.green_tripdata_partitoned
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT DATE(lpep_pickup_datetime) FROM sonic-airfoil-411903.ny_taxi.external_green_tripdata2 LIMIT 10

select count(*) from `sonic-airfoil-411903.ny_taxi.green_tripdata_partitoned`

select distinct(PULocationID) from `sonic-airfoil-411903.ny_taxi.external_green_tripdata`
0 

select distinct(PULocationID) from `sonic-airfoil-411903.ny_taxi.green_tripdata_non_partitoned`
where DATE(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30'
12.82 MB

select distinct(PULocationID) from `sonic-airfoil-411903.ny_taxi.green_tripdata_partitoned`
where DATE(lpep_pickup_datetime) between '2022-06-01' and '2022-06-30'
1.12 MB


---END of Homework

-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;

-- Impact of partition
-- Scanning 1.6GB of data
SELECT DISTINCT(VendorID)
FROM taxi-rides-ny.nytaxi.green_tripdata_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM taxi-rides-ny.nytaxi.green_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'green_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.green_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM taxi-rides-ny.nytaxi.external_green_tripdata;

-- Query scans 1.1 GB
SELECT count(*) as trips
FROM taxi-rides-ny.nytaxi.green_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM taxi-rides-ny.nytaxi.green_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;
