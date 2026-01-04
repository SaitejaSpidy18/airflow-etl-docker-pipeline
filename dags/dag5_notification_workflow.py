from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def risky_operation(**context):
    exec_date = context["execution_date"]
    day = exec_date.day
    if day % 5 == 0:
        raise Exception(f"Risky operation failed on day {day}")
    return {"status": "success", "execution_date": str(exec_date)}

def send_success_notification(context):
    return {
        "notification_type": "success",
        "status": "sent",
        "task_id": context["task_instance"].task_id,
        "timestamp": context["ts"],
    }

def send_failure_notification(context):
    return {
        "notification_type": "failure",
        "status": "sent",
        "task_id": context["task_instance"].task_id,
        "timestamp": context["ts"],
        "error": str(context.get("exception")),
    }

def cleanup_task(**context):
    return {
        "cleanup_status": "completed",
        "timestamp": datetime.utcnow().isoformat(),
    }

with DAG(
    dag_id="notification_and_error_handling",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    risky = PythonOperator(
        task_id="risky_operation",
        python_callable=risky_operation,
        provide_context=True,
        on_success_callback=send_success_notification,
        on_failure_callback=send_failure_notification,
    )

    cleanup = PythonOperator(
        task_id="always_execute_cleanup",
        python_callable=cleanup_task,
        trigger_rule="all_done",
        provide_context=True,
    )

    risky >> cleanup
