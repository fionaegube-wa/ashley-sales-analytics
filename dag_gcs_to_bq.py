# dag_gcs_to_bq.py  
from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration

# ===== Env vars set in Composer =====
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
REGION = os.getenv("GCP_REGION", "us-central1")
BUCKET = os.getenv("GCS_BUCKET", "gs://transaction_master")
BQ_DATASET = os.getenv("BQ_DATASET", "analytics")
BQ_TABLE = os.getenv("BQ_TABLE", "transactions")
SERVICE_ACCOUNT = os.getenv("SERVICE_ACCOUNT")  


# Default date-based path; can be overridden by dag_run.conf or Airflow Variable
DEFAULT_PARQUET_PATH = f"{BUCKET}/staging/transactions/{{{{ ds_nodash }}}}/transactions.parquet"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gcs_to_bq_batch",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    params={"default_input": DEFAULT_PARQUET_PATH},
    tags=["batch", "beam", "dataflow", "bq"],
) as dag:

    run_beam = BeamRunPythonPipelineOperator(
        task_id="dataflow_parquet_to_bq",
        # Beam runner selection
        runner="DataflowRunner",
        # Path to your Beam pipeline file in the Composer DAGs folder
        py_file="/home/airflow/gcs/dags/pipeline_parquet_to_bq.py",
        # Dataflow-specific config (job metadata, region, SA, etc.)
        dataflow_config=DataflowConfiguration(
            job_name="gcs-to-bq-{{ ds_nodash }}",
            location=REGION,
            service_account=SERVICE_ACCOUNT,
            # optional: wait for batch jobs to finish (default behavior is OK too)
            wait_until_finished=True,
        ),
        # Beam pipeline options: converted into --key=value at runtime
        pipeline_options={
            # Required Beam/Dataflow options
            "project": PROJECT_ID,
            "region": REGION,
            "staging_location": f"{BUCKET}/dataflow/staging",
            "temp_location": f"{BUCKET}/dataflow/temp",
            "network": "projects/ashley-furniture-358203/global/networks/subnet-",
            "subnetwork": "regions/us-central1/subnetworks/subnet-ashley",

            # Our pipeline knobs
            "input": "{{ dag_run.conf.get('input_uri') or var.value.SAMPLE_PARQUET_PATH or params.default_input }}",
            "output_table": f"{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}",
            "currency": "USD",
            # "lookup_path": f"{BUCKET}/lookups/product_map.jsonl",   # optional enrichment
            "deadletter_path": f"{BUCKET}/badrows/{{{{ ds_nodash }}}}/bad-*.jsonl",
            "required_fields": "transaction_id,product_id,quantity,unit_price",
            "require_non_negative": "true",
            "max_unit_price": "500.0",

            # BigQuery table design
            "output_write_disposition": "WRITE_APPEND",
            "output_create_disposition": "CREATE_IF_NEEDED",
            "bq_partition_field": "transaction_ts",
            "bq_cluster_fields": "product_id,customer_id",
        },
    )
