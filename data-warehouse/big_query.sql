----create an external table
CREATE OR REPLACE EXTERNAL TABLE `datacamp-412412.ny_taxi.external_trip_data`
OPTIONS (
  format = 'Parquet',
  uris = ['gs://datacamp-412412-bucket/trip_data/green_tripdata_2022-*.parquet']
);

SELECT COUNT(DISTINCT PULocationID) AS distinct_count FROM `datacamp-412412.ny_taxi.trip data`;
SELECT COUNT(DISTINCT PULocationID) AS distinct_count FROM `datacamp-412412.ny_taxi.external_trip_data`;

SELECT count(*) FROM `datacamp-412412.ny_taxi.trip data`
WHERE fare_amount = 0;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `datacamp-412412.ny_taxi.tripdata_partitoned`
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM `datacamp-412412.ny_taxi.external_trip_data`;

--partition by lpep_pickup_datetime cluster by PULocationID
CREATE OR REPLACE TABLE `datacamp-412412.ny_taxi.partitoned_clustered`
PARTITION BY DATE (lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM `datacamp-412412.ny_taxi.external_trip_data`;

--cluster on lpep_pickup_datetime partition by PULocationID
CREATE OR REPLACE TABLE `datacamp-412412.ny_taxi.clustered_partition`
PARTITION BY RANGE_BUCKET(PULocationID,  GENERATE_ARRAY(0, 255, 10))
CLUSTER BY lpep_pickup_datetime AS
SELECT * FROM `datacamp-412412.ny_taxi.external_trip_data`;

-- Impact of partition
--partition by datetime
SELECT DISTINCT(PULocationID )
FROM `datacamp-412412.ny_taxi.partitoned_clustered`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
ORDER BY PULocationID;

---partition by PULocationID
SELECT DISTINCT(PULocationID )
FROM `datacamp-412412.ny_taxi.clustered_partition`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
ORDER BY PULocationID;

--query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 on materialized table
SELECT DISTINCT(PULocationID )
FROM `datacamp-412412.ny_taxi.trip data`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
ORDER BY PULocationID;

--bonus
SELECT COUNT(*) FROM `datacamp-412412.ny_taxi.trip data`;

-- Scanning ~1.12 MB of DATA
SELECT DISTINCT(VendorID)
FROM `datacamp-412412.ny_taxi.tripdata_partitoned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- Let's look into the partitons:10mb
SELECT table_name, partition_id, total_rows
FROM `ny_taxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'tripdata_partitoned'
ORDER BY total_rows DESC;
