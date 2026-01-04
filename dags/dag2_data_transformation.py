from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

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
