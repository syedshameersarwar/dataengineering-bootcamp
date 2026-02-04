# Module 3: Data Warehousing & BigQuery

This module covers working with BigQuery and Google Cloud Storage to create external tables, materialized tables, and optimize queries using partitioning and clustering strategies.

## Setup

### Prerequisites

Before starting this module, ensure you have:
- A GCP project with BigQuery and Cloud Storage enabled
- A GCS bucket created (you can use the Terraform configuration from Module 1)
- A BigQuery dataset created
- A service account with appropriate permissions (GCS Admin and BigQuery Admin)
- The service account JSON key file downloaded

### Data Loading

For this homework, we use the Yellow Taxi Trip Records for January 2024 - June 2024 (6 months of data).

Parquet files are available from the [New York City Taxi Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

**Important:** Copy all 6 parquet files to your GCS bucket before proceeding. In the queries below, they are copied to `gs://<bucket>/yellowtrip-parquets`.

## Step 1: Create External Table

Create an external table in BigQuery that references the parquet files stored in your GCS bucket.

**Important:** Replace `<YOUR_PROJECT_ID>`, `<YOUR_DATASET>`, and `<YOUR_BUCKET_NAME>` with your actual GCP project ID, dataset name, and GCS bucket name.

```sql
CREATE OR REPLACE EXTERNAL TABLE `<YOUR_PROJECT_ID>.<YOUR_DATASET>.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://<YOUR_BUCKET_NAME>/yellowtrip-parquets/yellow_tripdata_2024-*.parquet']
);
```

## Step 2: Create Materialized Table

Create a regular (materialized) table in BigQuery using the Yellow Taxi Trip Records. Do not partition or cluster this table initially.

**Important:** Replace `<YOUR_PROJECT_ID>` and `<YOUR_DATASET>` with your actual GCP project ID and dataset name.

```sql
CREATE OR REPLACE TABLE `<YOUR_PROJECT_ID>.<YOUR_DATASET>.native_yellow_tripdata` AS
SELECT * FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.external_yellow_tripdata`;
```

## Step 3: Create Partitioned and Clustered Table

For Question 5, create an optimized table with partitioning and clustering. This will be used to demonstrate the benefits of partitioning in Question 6.

**Important:** Replace `<YOUR_PROJECT_ID>` and `<YOUR_DATASET>` with your actual GCP project ID and dataset name.

```sql
CREATE OR REPLACE TABLE `<YOUR_PROJECT_ID>.<YOUR_DATASET>.native_yellow_tripdata_partition_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.external_yellow_tripdata`;
```

## Quiz Questions and Answers

### Question 1: Counting Records

**What is count of records for the 2024 Yellow Taxi Data?**

**Query:**
```sql
SELECT COUNT(*) 
FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.external_yellow_tripdata`;
```

**Answer:** `20,332,093`

---

### Question 2: Data Read Estimation

**Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?**

**Queries:**

External Table:
```sql
SELECT COUNT(DISTINCT(PULocationID)) 
FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.external_yellow_tripdata`;
```

Materialized Table:
```sql
SELECT COUNT(DISTINCT(PULocationID)) 
FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.native_yellow_tripdata`;
```

**Answer:** `0 MB for the External Table and 155.12 MB for the Materialized Table`

**Explanation:** External tables in BigQuery don't charge for data scanning when querying metadata or performing certain operations, while materialized tables require scanning the actual data stored in BigQuery.

---

### Question 3: Understanding Columnar Storage

**Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?**

**Queries:**

Single column:
```sql
SELECT PULocationID 
FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.native_yellow_tripdata`;
```

Multiple columns:
```sql
SELECT PULocationID, DOLocationID 
FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.native_yellow_tripdata`;
```

**Answer:** `BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.`

---

### Question 4: Counting Zero Fare Trips

**How many records have a fare_amount of 0?**

**Query:**
```sql
SELECT COUNT(*) 
FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.native_yellow_tripdata` 
WHERE fare_amount = 0;
```

**Answer:** `8,333`

---

### Question 5: Partitioning and Clustering

**What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)?**

**Answer:** `Partition by tpep_dropoff_datetime and Cluster on VendorID`

**Explanation:** Partitioning by the datetime column used in filters allows BigQuery to skip entire partitions that don't match the filter condition, significantly reducing data scanned. Clustering by VendorID helps when ordering results by that column, as data is physically organized by the clustering key.

**Note:** In the actual implementation, we used `tpep_pickup_datetime` for partitioning as shown in Step 3, which demonstrates the same concept.

---

### Question 6: Partition Benefits

**Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?**

**Queries:**

Non-partitioned table:
```sql
SELECT DISTINCT(VendorID) 
FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.native_yellow_tripdata` 
WHERE tpep_pickup_datetime >= '2024-03-01' 
  AND tpep_pickup_datetime <= '2024-03-15';
```

Partitioned table:
```sql
SELECT DISTINCT(VendorID) 
FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.native_yellow_tripdata_partition_clustered` 
WHERE tpep_pickup_datetime >= '2024-03-01' 
  AND tpep_pickup_datetime <= '2024-03-15';
```

**Answer:** `310.24 MB for non-partitioned table and 26.84 MB for the partitioned table`

**Explanation:** The partitioned table scans significantly less data because BigQuery can skip partitions outside the date range (March 1-15, 2024), only scanning the relevant partitions. This demonstrates the cost and performance benefits of partitioning.

---

### Question 7: External Table Storage

**Where is the data stored in the External Table you created?**

**Answer:** `GCP Bucket`

**Explanation:** External tables in BigQuery reference data stored in external storage systems like Google Cloud Storage (GCS). The data remains in the GCS bucket and is not stored within BigQuery itself.

---

### Question 8: Clustering Best Practices

**It is best practice in Big Query to always cluster your data:**

**Answer:** `False`

**Explanation:** Clustering is not always necessary or beneficial. It should be used when:
- Your queries frequently filter or aggregate by specific columns
- The table is large (typically > 1 GB)
- The clustering columns have high cardinality
- You frequently order by the clustering columns

For small tables or tables with different query patterns, clustering may not provide benefits and can add overhead.

---

### Question 9: Understanding Table Scans

**No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?**

**Query:**
```sql
SELECT COUNT(*) 
FROM `<YOUR_PROJECT_ID>.<YOUR_DATASET>.native_yellow_tripdata`;
```

**Answer:** `0 bytes`

**Explanation:** BigQuery maintains metadata for native (materialized) tables, including row counts. When executing a `SELECT COUNT(*)` query, BigQuery can directly read the row count from its metadata without scanning the table or columns, resulting in 0 bytes processed.

---

## Summary

This module demonstrated:
- Creating external tables from GCS bucket data using parquet format
- Building materialized (native) tables in BigQuery
- Understanding the difference between external and materialized tables in terms of data storage and query costs
- Understanding columnar storage and how BigQuery only scans requested columns
- Implementing partitioning strategies to optimize query performance and reduce costs
- Implementing clustering strategies for query optimization
- Analyzing query execution plans and estimated bytes processed
- Understanding BigQuery metadata and how it enables zero-byte scans for certain operations

The module provided hands-on experience with BigQuery optimization techniques, showing how proper table design (partitioning and clustering) can significantly reduce query costs and improve performance when working with large datasets.

