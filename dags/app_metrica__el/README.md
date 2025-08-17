# App Metrica ETL DAG

This Apache Airflow DAG performs an ETL pipeline that extracts usage metrics data from a MinIO S3 bucket and loads it into ClickHouse.

## DAG ID

`app_metrica__el`

## Components

- **Source:** MinIO S3 bucket `app-metrica`
- **Destination:** ClickHouse cluster `default`
- **Orchestration:** Apache Airflow

## Tasks

1. **Create Base Table:** Creates a replicated ClickHouse table (`raw_usage_metrics_local`) for storing raw usage metrics.
2. **Create Distributed Table:** Creates a distributed table (`raw_usage_metrics`) to support distributed querying.
3. **Run Sling:** Executes a Sling job defined in `sling_minio_to_clickhouse.yaml` to load data from MinIO into ClickHouse.
