from datetime import datetime
import os

import pandas as pd
from sqlalchemy import create_engine

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator


def get_engine():
    """
    Build a clean SQLAlchemy engine from the airflow_db Postgres connection,
    without leaking Airflow's __extra__ field into the DSN.
    """
    hook = PostgresHook(postgres_conn_id="airflow_db")
    conn = hook.get_connection(hook.postgres_conn_id)

    user = conn.login
    password = conn.password
    host = conn.host
    port = conn.port or 5432
    schema = conn.schema or "airflow_db"

    uri = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{schema}"
    return create_engine(uri)


def check_table_exists():
    """
    Fail fast if transformed_employee_data does not exist or is not readable.
    """
    engine = get_engine()
    try:
        pd.read_sql("SELECT * FROM transformed_employee_data LIMIT 1;", con=engine)
    except Exception as e:
        raise ValueError("transformed_employee_data table missing") from e


def export_to_parquet():
    """
    Read all rows from transformed_employee_data and write them as a Parquet file
    into the mounted /opt/airflow/output directory.
    """
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM transformed_employee_data;", con=engine)

    output_dir = "/opt/airflow/output"
    os.makedirs(output_dir, exist_ok=True)

    ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"employee_data_{ts_str}.parquet")

    df.to_parquet(file_path, index=False)


def validate_parquet_file():
    """
    Simple validation: ensure at least one Parquet file exists and is readable.
    """
    output_dir = "/opt/airflow/output"
    files = [f for f in os.listdir(output_dir) if f.endswith(".parquet")]
    if not files:
        raise ValueError("No Parquet files found in output directory")

    files.sort()
    latest = os.path.join(output_dir, files[-1])
    df = pd.read_parquet(latest)
    if df.empty:
        raise ValueError("Latest Parquet file is empty")


with DAG(
    dag_id="postgres_to_parquet_export",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    check_transformed_table = PythonOperator(
        task_id="check_transformed_table",
        python_callable=check_table_exists,
    )

    export_task = PythonOperator(
        task_id="export_to_parquet",
        python_callable=export_to_parquet,
    )

    validate_task = PythonOperator(
        task_id="validate_parquet_file",
        python_callable=validate_parquet_file,
    )

    check_transformed_table >> export_task >> validate_task
