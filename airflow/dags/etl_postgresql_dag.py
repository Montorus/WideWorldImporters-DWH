from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from etl.extract.postgresql import extract_and_load


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def extract_and_load_adventureworks():
    extract_and_load(
        source_conn_id="postgres_source",
        dwh_conn_id="postgres_dwh",
        dag_name="etl_postgresql_adventureworks",
    )


with DAG(
    dag_id="etl_postgresql_adventureworks",
    default_args=default_args,
    description="ETL from PostgreSQL AdventureWorks to PostgreSQL DWH",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["postgresql", "etl", "adventureworks"],
) as dag:
    extract_load_task = PythonOperator(
        task_id="extract_and_load_adventureworks",
        python_callable=extract_and_load_adventureworks,
    )
