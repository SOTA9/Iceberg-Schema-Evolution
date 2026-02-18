# Ecommerce Lakehouse API Pipeline

## Project Overview

This project implements a **fully automated data pipeline** for ingesting, transforming, and loading e-commerce data from a **public API** into a **medallion architecture** (Bronze → Silver → Gold) using **Iceberg**, **Spark**, **GCS**, and **BigQuery**. The pipeline is orchestrated with **Astronomer / Cloud Airflow**.

---

## Key Features

- **Bronze ingestion** automatically detects new schema fields and applies them using Iceberg.
- **Idempotent**: safe to run daily without errors.
- **Snapshot logging** tracks table versions for historical reference.
- **Silver transformation** normalizes nested data and computes derived columns.
- **Gold layer** loads curated data into BigQuery for analytics.
- **Astronomer DAGs** schedule each layer of the pipeline.
- **GitLab CI/CD** automates testing, building, deployment, and pipeline triggers.
- Fully **production-ready** and compatible with immutable Parquet files in GCS.

---


## Setup Instructions

### 1. Configure GCP credentials

 - Place your GCP service account JSON in `configs/gcp_credentials.json` and ensure the environment variable is set:

```bash
  - export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/configs/gcp_credentials.json


# Navigate to project root
cd ecommerce-lakehouse-api

# Build Astronomer Docker image
docker build -t schema-evolution-iceberg:latest .

pip install -r requirements.txt


### Test scripts locally (optional)

# Bronze ingestion (with auto schema evolution)
python src/bronze_ingest_api.py

# Silver transformation
python src/silver_transform.py

# Gold load to BigQuery
python src/gold_load.py
