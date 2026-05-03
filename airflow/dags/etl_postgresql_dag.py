from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.extract_postgresql import extract_and_load

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def run_etl():
    extract_and_load(
        source_host="postgres_source",
        dwh_host="postgres_dwh",
        dag_name="etl_postgresql_adventureworks"
    )

with DAG(
    dag_id="etl_postgresql_adventureworks",
    default_args=default_args,
    description="ETL from AdventureWorks PostgreSQL source to PostgreSQL DWH",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["postgres", "etl", "adventureworks"],
) as dag:

    extract_load_task = PythonOperator(
        task_id="extract_and_load_adventureworks",
        python_callable=run_etl,
    )