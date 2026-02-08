-- Create  external table

CREATE OR REPLACE EXTERNAL TABLE `data-486808.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://nytaxi-data/yellow_tripdata_2024-01.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE data-486808.nytaxi.yellow_tripdata_non_partitioned AS
SELECT * FROM data-486808.nytaxi.external_yellow_tripdata;


--1-
SELECT COUNT(*) total_records
FROM data-486808.nytaxi.external_yellow_tripdata;

--2- 
--2.1
SELECT COUNT(DISTINCT PULocationID)
FROM data-486808.nytaxi.external_yellow_tripdata;

--2.2
SELECT COUNT(DISTINCT PULocationID)
FROM data-486808.nytaxi.yellow_tripdata_non_partitioned;


--3
SELECT COUNT(*)
FROM data-486808.nytaxi.yellow_tripdata_non_partitioned
WHERE fare_amount = 0;

--4

CREATE OR REPLACE TABLE data-486808.nytaxi.yellow_tripdata_optimized
PARTITION BY DATE(tpep_dropoff_datetime) CLUSTER BY VendorID
AS SELECT * FROM data-486808.nytaxi.yellow_tripdata_non_partitioned;




--5- 

SELECT COUNT(DISTINCT VendorID)
FROM data-486808.nytaxi.yellow_tripdata_non_partitioned
WHERE tpep_dropoff_datetime >= '2024-03-01'
AND tpep_dropoff_datetime < '2024-03-16';

SELECT COUNT(DISTINCT VendorID)
FROM data-486808.nytaxi.yellow_tripdata_optimized
WHERE tpep_dropoff_datetime >= '2024-03-01'
AND tpep_dropoff_datetime < '2024-03-16';