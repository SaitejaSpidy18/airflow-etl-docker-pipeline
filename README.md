# airflow-etl-docker-pipeline.

Project overview :

This project implements an end-to-end employee data pipeline using PostgreSQL, Apache Airflow, and Docker. It ingests CSV data into Postgres, applies transformations, and exports the results to Parquet for downstream analytics.
​

Setup :

Requirements :
Python 3.12 (with venv module enabled)
​

Docker Desktop (running in the background)
​

Git (for cloning and pulling the repository)
​

Clone the repository:
git clone https://github.com/SaitejaSpidy18/airflow-etl-pipeline.git
cd airflow-etl-pipeline.git

Create and activate virtual environment:
python -m venv .venv
.\.venv\Scripts\Activate.ps1

You should see (.venv) at the start of your terminal prompt indicating the environment is active.
​

Install dependencies:

pip install --upgrade pip
pip install -r requirements.txt
This installs all Python libraries needed for local utilities and running the test suite.
​

Running the stack (Docker + Airflow)
Start services

From the project root (with Docker Desktop running):
docker-compose up -d --build

This command builds and starts:

PostgreSQL database (postgres service).

Airflow webserver and scheduler with your DAGs mounted from the dags/ directory.
​

Access Airflow UI
Once containers are healthy:

URL: http://localhost:8080

Default login (as configured in docker-compose.yml):

Username: admin

Password: admin

You can enable and trigger each DAG from the DAGs page in the Airflow UI.
​

Stop services
To stop and remove the containers:

docker-compose down

DAGs : 
The project defines five DAGs under dags/:

DAG 1 – CSV to Postgres

dag_id="employee_data_ingestion"

Reads employee data from a CSV file and loads it into the raw_employee_data table in PostgreSQL.
​

DAG 2 – Data transformation pipeline

dag_id="data_transformation_pipeline"

Reads from raw_employee_data, enriches the dataset with derived fields (age group, salary category, year joined), and writes results to transformed_employee_data.
​

DAG 3 – Postgres to Parquet export

dag_id="postgres_to_parquet_export"

Validates that transformed_employee_data exists, exports it to a timestamped Parquet file under /opt/airflow/output, and performs a simple read-back validation.
​

DAG 4 – Conditional workflow by day

dag_id="conditional_workflow_by_day"

Uses a BranchPythonOperator to choose between weekday, weekend, or end-of-week processing paths based on the execution date.
​

DAG 5 – Notification and error handling

dag_id="notification_and_error_handling"

Demonstrates success and failure handling patterns (e.g., simulated failure, follow-up notification/cleanup tasks) to showcase operational workflows in Airflow.
​

Each DAG can be toggled On and manually triggered from the Airflow web UI to verify behavior.

Testing:

This project includes a minimal test suite under the tests/ directory to verify the environment and basic setup.

To run all tests (with your virtual environment active):
python -m pytest tests -v

This command:

Discovers all test_*.py files under tests/.

Executes them with verbose output so you can see each individual test result.
​

All tests should pass before creating a submission tag or sharing the repository with reviewers.