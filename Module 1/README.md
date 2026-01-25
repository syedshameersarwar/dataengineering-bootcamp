# Module 1 Homework: Docker & SQL - Solution

This repository contains my solution for Module 1 of the Data Engineering Zoomcamp, covering Docker, SQL, and Terraform.

## Table of Contents

- [Question 1: Understanding Docker images](#question-1-understanding-docker-images)
- [Question 2: Understanding Docker networking and docker-compose](#question-2-understanding-docker-networking-and-docker-compose)
- [Question 3: Counting short trips](#question-3-counting-short-trips)
- [Question 4: Longest trip for each day](#question-4-longest-trip-for-each-day)
- [Question 5: Biggest pickup zone](#question-5-biggest-pickup-zone)
- [Question 6: Largest tip](#question-6-largest-tip)
- [Question 7: Terraform Workflow](#question-7-terraform-workflow)
- [Data Ingestion](#data-ingestion)
- [Infrastructure Setup](#infrastructure-setup)

---

## Question 1: Understanding Docker images

**Question:** Run docker with the `python:3.13` image. Use an entrypoint `bash` to interact with the container. What's the version of `pip` in the image?

**Answer:** `25.3`

**Command:**
```bash
docker run -it --entrypoint bash python:3.13
pip --version
```

---

## Question 2: Understanding Docker networking and docker-compose

**Question:** Given the `docker-compose.yaml`, what is the `hostname` and `port` that pgadmin should use to connect to the postgres database?

**Answer:** `db:5432`

**Explanation:** In Docker Compose, services can communicate with each other using the service name as the hostname. The `db` service is accessible at `db:5432` (service name:internal port), not `localhost:5432` or the mapped external port.

---

## Question 3: Counting short trips

**Question:** For the trips in November 2025 (`lpep_pickup_datetime` between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a `trip_distance` of less than or equal to 1 mile?

**Answer:** `8,007`

**SQL Query:**
```sql
SELECT COUNT(*) 
FROM public.green_tripdata 
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;
```

---

## Question 4: Longest trip for each day

**Question:** Which was the pick up day with the longest trip distance? Only consider trips with `trip_distance` less than 100 miles (to exclude data errors).

**Answer:** `2025-11-14`

**SQL Query:**
```sql
SELECT 
    DATE(lpep_pickup_datetime) AS trip_date, 
    MAX(trip_distance) AS max_distance
FROM public.green_tripdata 
WHERE trip_distance < 100
GROUP BY trip_date 
ORDER BY max_distance DESC 
LIMIT 1;
```

---

## Question 5: Biggest pickup zone

**Question:** Which was the pickup zone with the largest `total_amount` (sum of all trips) on November 18th, 2025?

**Answer:** `East Harlem North`

**SQL Query:**
```sql
SELECT 
    zl."Zone", 
    ROUND(SUM(td.total_amount)::numeric, 3) AS total_amount_sum 
FROM public.green_tripdata td
INNER JOIN public.taxi_zone_lookup zl 
    ON zl."LocationID" = td."PULocationID"
WHERE DATE(td.lpep_pickup_datetime) = '2025-11-18'
GROUP BY zl."Zone" 
ORDER BY total_amount_sum DESC 
LIMIT 1;
```

---

## Question 6: Largest tip

**Question:** For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

**Answer:** `Yorkville West`

**SQL Query:**
```sql
SELECT 
    zl."Zone" AS drop_off_zone, 
    ROUND(MAX(td.tip_amount)::numeric, 3) AS tip_amount 
FROM public.green_tripdata td
INNER JOIN public.taxi_zone_lookup zl 
    ON zl."LocationID" = td."DOLocationID"
WHERE DATE(td.lpep_pickup_datetime) >= '2025-11-01' 
  AND DATE(td.lpep_pickup_datetime) <= '2025-11-30'
  AND td."PULocationID" = (
      SELECT zl."LocationID" 
      FROM public.taxi_zone_lookup zl  
      WHERE zl."Zone" = 'East Harlem North'
  )
GROUP BY zl."Zone" 
ORDER BY tip_amount DESC 
LIMIT 1;
```

---

## Question 7: Terraform Workflow

**Question:** Which of the following sequences, respectively, describes the workflow for:
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform

**Answer:** `terraform init, terraform apply -auto-approve, terraform destroy`

---

## Docker Compose Setup

Before ingesting data, you need to start the PostgreSQL database and pgAdmin services using Docker Compose.

The `docker-compose.yaml` file defines:
- **PostgreSQL** service on port `5444:5432`
- **pgAdmin** service on port `8090:80`
- Persistent volumes for database and pgAdmin data

**Start services:**
```bash
docker-compose up -d
```

**Verify services are running:**
```bash
docker-compose ps
```

**Stop services:**
```bash
docker-compose down
```

**Note:** The database will be available at `localhost:5444` from your host machine. Use this port when running the ingestion script.

---

## Data Ingestion

### Environment Setup with UV

This project uses [uv](https://github.com/astral-sh/uv) for fast Python package management. The project dependencies are defined in `pyproject.toml`.

**Setup Steps:**

1. **Install uv** (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Install project dependencies:**
   ```bash
   uv sync
   ```

3. **Activate the virtual environment:**
   ```bash
   source .venv/bin/activate
   ```

   Or run commands directly with uv:
   ```bash
   uv run <command>
   ```

### Data Ingestion Script

The `ingest.py` script loads taxi trip data and zone lookup data from local files into PostgreSQL tables.

**Script Features:**
- Automatically detects file type (Parquet or CSV)
- Creates table schema if it doesn't exist
- Handles both `green_tripdata_2025-11.parquet` and `taxi_zone_lookup.csv`

**Usage:**

```bash
uv run python ingest.py \
  --user postgres \
  --password postgres \
  --host localhost \
  --port 5444 \
  --database ny_taxi
```

**Arguments:**
- `--user`: PostgreSQL username (default: `postgres`)
- `--password`: PostgreSQL password (default: `postgres`)
- `--host`: PostgreSQL host address (default: `localhost`)
- `--port`: PostgreSQL port number (default: `5432`)
- `--database`: PostgreSQL database name (default: `ny_taxi`)


**How it works:**
1. The script reads files from the `data/` directory
2. For Parquet files, it uses `pandas.read_parquet()`
3. For CSV files, it uses `pandas.read_csv()`
4. Creates table schema using `to_sql()` with `if_exists='replace'`
5. Inserts data using `to_sql()` with `if_exists='append'`

**Files ingested:**
- `data/green_tripdata_2025-11.parquet` → `green_tripdata` table
- `data/taxi_zone_lookup.csv` → `taxi_zone_lookup` table

---

## Infrastructure Setup

### Terraform Configuration

The `infra/` folder contains Terraform configuration files for provisioning GCP resources.

**File Structure:**
```
infra/
├── main.tf          # Main Terraform configuration
├── vars.tf          # Variable definitions
├── dev.tfvars       # Development environment variables (not committed)
├── terraform.tfstate # Terraform state file
└── terraform.tfstate.backup # State backup
```

**Configuration Files:**

1. **`main.tf`**: Defines the Terraform provider and resources:
   - Google Cloud Provider (version 5.6.0)
   - GCS Bucket (`google_storage_bucket`) with lifecycle rules
   - BigQuery Dataset (`google_bigquery_dataset`)

2. **`vars.tf`**: Declares input variables:
   - `project`: GCP Project ID
   - `region`: GCP Region
   - `location`: Resource location
   - `bq_dataset_name`: BigQuery dataset name
   - `gcs_bucket_name`: GCS bucket name
   - `gcs_storage_class`: Bucket storage class

3. **`dev.tfvars`**: Contains actual values for development (create this file locally):
   ```hcl
   project = "your-gcp-project-id"
   region = "us-central1"
   location = "US"
   bq_dataset_name = "ny_taxi_dataset"
   gcs_bucket_name = "your-unique-bucket-name"
   ```

**Deployment Steps:**

1. **Create `dev.tfvars` file:**
   ```bash
   cd infra
   ```
   
   Create a `dev.tfvars` file with your GCP configuration:
   ```hcl
   project = "your-gcp-project-id"
   region = "us-central1"
   location = "US"
   bq_dataset_name = "ny_taxi_dataset"
   gcs_bucket_name = "your-unique-bucket-name"
   ```
   
   **Note:** Replace the placeholder values with your actual GCP project details. The `gcs_bucket_name` must be globally unique.

2. **Initialize Terraform:**
   ```bash
   terraform init
   ```

3. **Review planned changes:**
   ```bash
   terraform plan -var-file=dev.tfvars
   ```

4. **Apply configuration:**
   ```bash
   terraform apply -var-file=dev.tfvars -auto-approve
   ```

5. **Destroy resources (when done):**
   ```bash
   terraform destroy -var-file=dev.tfvars
   ```

**Resources Created:**
- **GCS Bucket**: Created with the name specified in `dev.tfvars` in the location specified
  - Lifecycle rule: Aborts incomplete multipart uploads after 1 day
  - Force destroy enabled for easy cleanup
- **BigQuery Dataset**: Created with the name specified in `dev.tfvars` in the location specified

---

## Project Dependencies

Dependencies are managed via `pyproject.toml`:
- `fastparquet>=2025.12.0`: Parquet file reading
- `pandas>=3.0.0`: Data manipulation
- `psycopg2>=2.9.11`: PostgreSQL adapter
- `sqlalchemy>=2.0.46`: Database toolkit

