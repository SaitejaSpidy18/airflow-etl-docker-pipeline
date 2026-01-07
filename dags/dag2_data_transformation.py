from datetime import datetime
from typing import List, Tuple

import pandas as pd
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator


def transform_employee_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["full_info"] = df["name"] + " - " + df["city"]

    def age_group(age):
        if age < 30:
            return "Young"
        elif age < 50:
            return "Mid"
        else:
            return "Senior"

    def salary_category(sal):
        if sal < 50000:
            return "Low"
        elif sal < 80000:
            return "Medium"
        else:
            return "High"

    df["age_group"] = df["age"].apply(age_group)
    df["salary_category"] = df["salary"].apply(salary_category)
    df["year_joined"] = pd.to_datetime(df["join_date"]).dt.year
    return df


def create_transformed_table_in_db() -> None:
    hook = PostgresHook(postgres_conn_id="airflow_db")

    create_sql = """
    DROP TABLE IF EXISTS transformed_employee_data;

    CREATE TABLE transformed_employee_data (
        id              INT PRIMARY KEY,
        name            TEXT,
        age             INT,
        city            TEXT,
        salary          NUMERIC,
        join_date       DATE,
        full_info       TEXT,
        age_group       TEXT,
        salary_category TEXT,
        year_joined     INT
    );
    """
    hook.run(create_sql)


def transform_and_load_employee_data() -> None:
    hook = PostgresHook(postgres_conn_id="airflow_db")

    # 1) Read from the raw employee table created by DAG 1
    src_df = hook.get_pandas_df(
        "SELECT id, name, age, city, salary, join_date FROM raw_employee_data;"
    )

    # 2) Apply transformations
    transformed_df = transform_employee_df(src_df)

    # 3) Truncate target table before loading (safe to rerun)
    hook.run("TRUNCATE TABLE transformed_employee_data;")

    # 4) Prepare rows for insert
    rows: List[Tuple] = [
        (
            int(row["id"]),
            row["name"],
            int(row["age"]),
            row["city"],
            float(row["salary"]),
            row["join_date"],
            row["full_info"],
            row["age_group"],
            row["salary_category"],
            int(row["year_joined"]),
        )
        for _, row in transformed_df.iterrows()
    ]

    insert_sql = """
    INSERT INTO transformed_employee_data (
        id,
        name,
        age,
        city,
        salary,
        join_date,
        full_info,
        age_group,
        salary_category,
        year_joined
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    conn = hook.get_conn()
    with conn, conn.cursor() as cur:
        cur.executemany(insert_sql, rows)


with DAG(
    dag_id="data_transformation_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    create_transformed_table = PythonOperator(
        task_id="create_transformed_table",
        python_callable=create_transformed_table_in_db,
    )

    transform_and_load = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load_employee_data,
    )

    create_transformed_table >> transform_and_load
