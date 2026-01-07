from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

def create_employee_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    create_sql = """
    CREATE TABLE IF NOT EXISTS raw_employee_data (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255),
        age INTEGER,
        city VARCHAR(100),
        salary NUMERIC(12,2),
        join_date DATE
    );
    """
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()

def truncate_employee_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE raw_employee_data;")
        conn.commit()

def load_csv_data():
    hook = PostgresHook(postgres_conn_id="postgres_default")

    df = pd.read_csv("/opt/airflow/data/input.csv")

    # Ensure plain Python types, not NumPy dtypes
    df = df.astype(
        {
            "id": "int64",
            "name": "string",
            "age": "int64",
            "city": "string",
            "salary": "float64",
            "join_date": "string",
        }
    )

    rows = [
        (
            int(row["id"]),
            str(row["name"]),
            int(row["age"]),
            str(row["city"]),
            float(row["salary"]),
            str(row["join_date"]),
        )
        for _, row in df.iterrows()
    ]

    insert_sql = """
    INSERT INTO raw_employee_data (id, name, age, city, salary, join_date)
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)
        conn.commit()

    return len(rows)


with DAG(
    dag_id="csv_to_postgres_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl"],
) as dag:

    create_table_task = PythonOperator(
        task_id="create_employee_table",
        python_callable=create_employee_table,
    )

    truncate_table_task = PythonOperator(
        task_id="truncate_employee_table",
        python_callable=truncate_employee_table,
    )

    load_csv_task = PythonOperator(
        task_id="load_csv_data",
        python_callable=load_csv_data,
    )

    create_table_task >> truncate_table_task >> load_csv_task
