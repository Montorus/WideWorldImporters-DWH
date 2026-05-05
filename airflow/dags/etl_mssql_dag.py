from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from etl.extract.mssql import extract_and_load


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def extract_and_load_customers():
    extract_and_load(
        source_conn_id="mssql_source",
        dwh_conn_id="postgres_dwh",
        dag_name="etl_mssql_customers",
    )


with DAG(
    dag_id="etl_mssql_customers",
    default_args=default_args,
    description="ETL from MSSQL WideWorldImporters to PostgreSQL DWH",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mssql", "etl", "customers"],
) as dag:
    extract_load_task = PythonOperator(
        task_id="extract_and_load_customers",
        python_callable=extract_and_load_customers,
    )
