"""
recon_pipeline_dag.py
Orchestrates the ClearVault Corp reconciliation pipeline.
Downloads CSVs from S3 and loads them into Snowflake RAW layer.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import boto3
import pandas as pd
import os

# ----- Configuration -----
S3_BUCKET = "clearvault-raw-data"
S3_PREFIX = "raw"
LOCAL_TMP = "/tmp/clearvault"
SNOWFLAKE_CONN_ID = "snowflake_default"

SOURCES = ["bank", "erp", "billing"]

default_args = {
    "owner": "clearvault",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# ----- Task functions -----

def create_raw_tables():
    """Creates RAW tables in Snowflake if they don't exist."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    create_sql = """
    CREATE TABLE IF NOT EXISTS RECONCILIATION_DB.RAW.{table} (
        transaction_id      VARCHAR(20),
        customer_id         VARCHAR(20),
        facility_id         VARCHAR(20),
        amount              FLOAT,
        transaction_date    DATE,
        payment_method      VARCHAR(20),
        status              VARCHAR(20),
        source              VARCHAR(20),
        _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """

    for source in SOURCES:
        table_name = f"raw_{source}_transactions"
        hook.run(create_sql.format(table=table_name))
        print(f"Table {table_name} ready.")


def download_from_s3():
    """Downloads all CSVs from S3 to a temp directory inside the container."""
    os.makedirs(LOCAL_TMP, exist_ok=True)

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    )

    for source in SOURCES:
        filename = f"{source}_transactions.csv"
        s3_key = f"{S3_PREFIX}/{filename}"
        local_path = f"{LOCAL_TMP}/{filename}"

        print(f"Downloading s3://{S3_BUCKET}/{s3_key}...")
        s3.download_file(S3_BUCKET, s3_key, local_path)
        print(f"Saved to {local_path}")


def load_source_to_snowflake(source: str):
    """Loads a single source CSV into its RAW Snowflake table."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    local_path = f"{LOCAL_TMP}/{source}_transactions.csv"
    table_name = f"RECONCILIATION_DB.RAW.raw_{source}_transactions"

    df = pd.read_csv(local_path)
    print(f"Loaded {len(df)} rows from {local_path}")

    # Truncate before loading — full refresh for this pipeline
    hook.run(f"TRUNCATE TABLE {table_name};")

    # Insert rows in batches of 500
    conn = hook.get_conn()
    cursor = conn.cursor()
    # Explicitly set warehouse for this session
    cursor.execute("USE WAREHOUSE RECON_WH;")

    batch_size = 500
    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        placeholders = ",".join(["%s"] * len(df.columns))
        insert_sql = f"INSERT INTO {table_name} (transaction_id, customer_id, facility_id, amount, transaction_date, payment_method, status, source) VALUES ({placeholders})"
        cursor.executemany(insert_sql, batch)

    conn.commit()
    cursor.close()
    print(f"Loaded {len(rows)} rows into {table_name}")


# ----- DAG definition -----

with DAG(
    dag_id="clearvault_recon_pipeline",
    default_args=default_args,
    description="ClearVault Corp — ingestion from S3 to Snowflake RAW",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual trigger for now
    catchup=False,
    tags=["clearvault", "ingestion", "snowflake"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    create_tables = PythonOperator(
        task_id="create_raw_tables",
        python_callable=create_raw_tables,
    )

    download_s3 = PythonOperator(
        task_id="download_from_s3",
        python_callable=download_from_s3,
    )

    load_tasks = []
    for source in SOURCES:
        task = PythonOperator(
            task_id=f"load_{source}_to_snowflake",
            python_callable=load_source_to_snowflake,
            op_kwargs={"source": source},
        )
        load_tasks.append(task)

    # Define task dependencies
    start >> create_tables >> download_s3 >> load_tasks >> end