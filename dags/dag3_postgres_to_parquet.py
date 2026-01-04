from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import os

def check_table_exists():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()
    try:
        df = pd.read_sql("SELECT * FROM transformed_employee_data", con=engine)
    except Exception as e:
        raise ValueError("transformed_employee_data table missing") from e
    if df.empty:
        raise ValueError("transformed_employee_data table is empty")
    return len(df)

def export_table_to_parquet(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()
    df = pd.read_sql("SELECT * FROM transformed_employee_data", con=engine)

    exec_date = context["ds_nodash"]
    out_path = f"/opt/airflow/output/employee_data_{exec_date}.parquet"
    df.to_parquet(out_path, engine="pyarrow", compression="snappy")

    size = os.path.getsize(out_path)
    return {"file_path": out_path, "row_count": len(df), "file_size_bytes": size}

def validate_parquet(**context):
    ti = context["ti"]
    info = ti.xcom_pull(task_ids="export_to_parquet")
    file_path = info["file_path"]

    df = pd.read_parquet(file_path)
    if df.empty:
        raise ValueError("Parquet file is empty")
    expected_cols = {
        "id","name","age","city","salary","join_date",
        "full_info","age_group","salary_category","year_joined",
    }
    if not expected_cols.issubset(df.columns):
        raise ValueError("Parquet file missing expected columns")
    return True

with DAG(
    dag_id="postgres_to_parquet_export",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl"],
) as dag:

    check_table = PythonOperator(
        task_id="check_transformed_table",
        python_callable=check_table_exists,
    )

    export = PythonOperator(
        task_id="export_to_parquet",
        python_callable=export_table_to_parquet,
        provide_context=True,
    )

    validate = PythonOperator(
        task_id="validate_parquet_file",
        python_callable=validate_parquet,
        provide_context=True,
    )

    check_table >> export >> validate
