from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def extract_and_load():
    mssql_engine = create_engine(
        "mssql+pymssql://sa:StrongPass123!@mssql_source:1433/WideWorldImporters"
    )
    dwh_engine = create_engine(
        "postgresql://dwh_user:dwh123@postgres_dwh:5432/warehouse_db"
    )

    df = pd.read_sql("""
        SELECT
            CustomerID,
            CustomerName,
            CreditLimit,
            IsStatementSent,
            IsOnCreditHold
        FROM Sales.Customers
    """, mssql_engine)

    print(f"Ištraukta eilučių: {len(df)}")

    df.to_sql(
        name="raw_customers",
        con=dwh_engine,
        schema="public",
        if_exists="replace",
        index=False
    )

    print("Įkelta į DWH sėkmingai!")

with DAG(
    dag_id="etl_mssql_customers",
    default_args=default_args,
    description="ETL iš MSSQL WideWorldImporters į PostgreSQL DWH",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mssql", "etl", "customers"],
) as dag:

    extract_load_task = PythonOperator(
        task_id="extract_and_load_customers",
        python_callable=extract_and_load,
    )