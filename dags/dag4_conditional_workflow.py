from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

def determine_branch(**context):
    exec_date = context["execution_date"]
    dow = exec_date.weekday()  # 0=Mon ... 6=Sun
    if dow <= 2:
        return "weekday_processing"
    elif dow <= 4:
        return "end_of_week_processing"
    else:
        return "weekend_processing"

def weekday_process(**context):
    return {
        "day_name": context["execution_date"].strftime("%A"),
        "task_type": "weekday",
    }

def end_of_week_process(**context):
    return {
        "day_name": context["execution_date"].strftime("%A"),
        "task_type": "end_of_week",
    }

def weekend_process(**context):
    return {
        "day_name": context["execution_date"].strftime("%A"),
        "task_type": "weekend",
    }

with DAG(
    dag_id="conditional_workflow_by_day",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    branch = BranchPythonOperator(
        task_id="branch_by_day",
        python_callable=determine_branch,
    )

    weekday_task = PythonOperator(
        task_id="weekday_processing",
        python_callable=weekday_process,
    )

    end_of_week_task = PythonOperator(
        task_id="end_of_week_processing",
        python_callable=end_of_week_process,
    )

    weekend_task = PythonOperator(
        task_id="weekend_processing",
        python_callable=weekend_process,
    )

    join = EmptyOperator(task_id="end")

    start >> branch
    branch >> [weekday_task, end_of_week_task, weekend_task] >> join
