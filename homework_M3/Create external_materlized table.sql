-- Create  external table

CREATE OR REPLACE EXTERNAL TABLE `data-486808.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://nytaxi-data/yellow_tripdata_2024-01.parquet']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE data-486808.nytaxi.yellow_tripdata_non_partitioned AS
SELECT * FROM data-486808.nytaxi.external_yellow_tripdata;