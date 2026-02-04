# data-engineering-zoomcamp
DE ProDev workshops


##  Module 03 Queries 
Q1 & 2
```
SELECT COUNT(*) 
FROM `zoomcamp03.yellowtaxi2024`;

SELECT COUNT(*) 
FROM `zoomcamp03.external_yellowtaxi2024`;
```

Q4
```
SELECT COUNT(1) 
FROM `zoomcamp03.yellowtaxi2024`
WHERE fare_amount = 0;
```

Q5
```
CREATE OR REPLACE TABLE de-course-xxxxxx.zoomcamp03.yellowtaxi2024_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM de-course-xxxxxx.zoomcamp03.external_yellowtaxi2024;
```

Q6
```
SELECT DISTINCT VendorID
FROM `zoomcamp03.yellowtaxi2024`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' and '2024-03-15';

SELECT DISTINCT VendorID
FROM `zoomcamp03.yellowtaxi2024_partitioned_clustered`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' and '2024-03-15';
```
