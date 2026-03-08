# Module 6: Batch Processing with Apache Spark

This module covers batch processing using Apache Spark and PySpark. The homework involves reading, repartitioning, and analyzing the Yellow Taxi Trip Records for November 2025 using Spark DataFrames and Spark SQL.

## Setup

### Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) for dependency management
- Java (JDK 17 or 21) installed and `JAVA_HOME` set

### Installation

This project uses `uv` to manage dependencies. From the `Module 6/` directory:

```bash
uv sync
```

This installs the dependencies defined in [`pyproject.toml`](./pyproject.toml):

- `pyspark>=4.1.1`
- `marimo>=0.20.4`

### Data

Download the required datasets into the `data/` directory:

```bash
wget -P data/ https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
wget -P data/ https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

### Running

Verify your Spark installation:

```bash
uv run python test_spark.py
```

Run the interactive marimo notebook:

```bash
uv run marimo edit homework.py
```

## Homework Questions and Solutions

### Question 1: Install Spark and PySpark

**Question:** Install Spark, run PySpark, create a local Spark session, and execute `spark.version`. What's the output?

**Answer:** `4.1.1`

**Solution:** A local Spark session is created in [`test_spark.py`](./test_spark.py) and the version is printed:

```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

print(f"Spark version: {spark.version}")
```

---

### Question 2: Yellow November 2025

**Question:** Read the November 2025 Yellow data into a Spark DataFrame. Repartition to 4 partitions and save as parquet. What is the average size of the parquet files that were created (in MB)?

**Answer:** `25 MB`

**Solution:** The data is read, repartitioned, and written to parquet in [`homework.py`](./homework.py):

```python
df_yellow = spark.read.parquet('data/yellow_tripdata_2025-11.parquet')
df_yellow.repartition(4).write.parquet("data/processed/")
```

File sizes are verified with:

```python
result = subprocess.run(
    ["ls", "-la", "--block-size=M", "./data/processed/"],
    capture_output=True, text=True
)
print(result.stdout)
```

Each of the 4 `.parquet` files is approximately 25 MB.

---

### Question 3: Count Records

**Question:** How many taxi trips were there on the 15th of November? Consider only trips that started on the 15th.

**Answer:** `162,604`

**Solution:** Trips are filtered by pickup date using `to_date()`:

```python
from pyspark.sql import functions as F

count = df_yellow.filter(
    F.to_date("tpep_pickup_datetime") == "2025-11-15"
).count()
```

---

### Question 4: Longest Trip

**Question:** What is the length of the longest trip in the dataset in hours?

**Answer:** `90.6`

**Solution:** Trip duration is computed as the difference between dropoff and pickup timestamps, converted to hours:

```python
longest_trip = (
    df_yellow.withColumn(
        "diff_hours",
        (F.unix_timestamp("tpep_dropoff_datetime") -
         F.unix_timestamp("tpep_pickup_datetime")) / 3600
    )
    .orderBy(F.col("diff_hours").desc())
    .limit(1)
)
longest_trip.show()
```

---

### Question 5: User Interface

**Question:** Spark's User Interface which shows the application's dashboard runs on which local port?

**Answer:** `4040`

**Explanation:** The Spark UI is accessible at `http://localhost:4040` by default when a Spark session is active. It provides dashboards for monitoring jobs, stages, storage, environment, and executors.

---

### Question 6: Least Frequent Pickup Location Zone

**Question:** Using the zone lookup data and the Yellow November 2025 data, what is the name of the least frequent pickup location zone?

**Answer:** `Governor's Island/Ellis Island/Liberty Island` and `Arden Heights` (tied)

**Solution:** Both datasets are registered as temporary views and queried using Spark SQL with `DENSE_RANK()` to find the zone(s) with the fewest pickups:

```sql
WITH pickup_counts AS (
    SELECT
        z.Zone AS pickup_zone,
        COUNT(*) AS cnt
    FROM yellow_tripdata t
    JOIN zone_lookup z
        ON t.PULocationID = z.LocationID
    GROUP BY z.Zone
),
ranked_pickups AS (
    SELECT
        pickup_zone,
        cnt,
        DENSE_RANK() OVER (ORDER BY cnt ASC) AS rnk
    FROM pickup_counts
)
SELECT pickup_zone, cnt
FROM ranked_pickups
WHERE rnk = 1;
```

Both zones have the same lowest count, hence `DENSE_RANK()` returns both.

---

## Project Structure

```
Module 6/
├── data/
│   ├── yellow_tripdata_2025-11.parquet   # Raw trip data
│   ├── taxi_zone_lookup.csv              # Zone reference data
│   └── processed/                        # Repartitioned parquet output
├── homework.py          # Marimo notebook with all homework solutions
├── test_spark.py        # Spark installation verification script
├── pyproject.toml       # Project dependencies (uv)
├── uv.lock              # Locked dependency versions
├── answers.md           # Short-form answers
├── question.md          # Original homework questions
└── README.md            # This file
```

## Summary

This module demonstrated:

- Installing and configuring Apache Spark with PySpark
- Creating local Spark sessions for batch processing
- Reading and writing parquet files with repartitioning
- Filtering and aggregating large datasets using the DataFrame API
- Computing derived columns (trip duration) with `unix_timestamp`
- Using Spark SQL with temporary views, CTEs, and window functions
- Monitoring Spark applications via the Spark UI
- Using marimo as an interactive notebook environment for PySpark

---

## References

- [PySpark Installation Guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/06-batch/setup/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Marimo Documentation](https://docs.marimo.io/)
- [Original Homework Questions](./question.md)
