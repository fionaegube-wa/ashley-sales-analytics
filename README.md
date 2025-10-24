#  Ashley Sales Data Analytics
Overview

This project demonstrates how Ashley Furniture automated its sales data ingestion and transformation process using Google Cloud Dataflow (Apache Beam) and Cloud Composer (Airflow).

The pipeline ingests parquet files from Google Cloud Storage (GCS), applies data validation, standardization, and enrichment, and then loads the clean data into BigQuery for downstream analytics and reporting.

Architecture

GCS (Parquet Sales Files) -> Dataflow (Validation & Standardization, Currency Normalization, Product & Customer Enrichment) -> BigQuery (Partitioned & Clustered Fact Table) -> Cloud Composer (Orchestration)

Key Features

- Automated ingestion of daily parquet sales files from GCS.

- Validation checks on required fields (e.g. transaction ID, unit price).

- Standardization of currency fields and enrichment with product and customer attributes.

- Dead-letter handling to route invalid rows for troubleshooting.

- Loads enriched data into BigQuery as a partitioned and clustered fact table.

- Fully orchestrated with Cloud Composer, supporting scheduled daily runs.
  

Tech Stack

- Python 3.9+

- Apache Beam (Dataflow)

- Google Cloud Storage (GCS)

- BigQuery

- Airflow (Cloud Composer)
  

How It Works

1. Dataflow Pipeline (pipeline_parquet_to_bq.py): Reads parquet files from a configured GCS bucket, performs validation, transformation, and enrichment logic in Apache Beam, outputs clean data into BigQuery and invalid records to a bad-rows path in GCS.

2. DAG (dag_gcs_to_bq.py): Schedules and orchestrates the Dataflow job and supports environment variables for project, region, service account, staging paths, and table references.

Allows both manual and scheduled triggering.
analytics.transactions \
  --deadletter_path gs://dataflow-staging/badrows \
  --bq_partition_field transaction_ts \
  --bq_cluster_fields product_id,customer_id
