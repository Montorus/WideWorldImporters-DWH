from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def extract_and_load():
    # Connect to AdventureWorks PostgreSQL source
    source_engine = create_engine(
        "postgresql://postgres:postgres123@postgres_source:5432/source_db"
    )

    # Connect to Data Warehouse
    dwh_engine = create_engine(
        "postgresql://dwh_user:dwh123@postgres_dwh:5432/warehouse_db"
    )

    # Define tables to extract and their target names in DWH
    tables = {
        "raw_sales_orders": "SELECT * FROM sales.salesorderheader",
        "raw_sales_details": "SELECT * FROM sales.salesorderdetail",
        "raw_employees": "SELECT * FROM humanresources.employee",
        "raw_products": "SELECT * FROM production.product",
    }

    # Extract each table from source and load into DWH
    # Using TRUNCATE + append instead of replace to preserve views that depend on raw tables
    for table_name, query in tables.items():
        df = pd.read_sql(query, source_engine)

        with dwh_engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE public.{table_name}"))
            conn.commit()

        df.to_sql(
            name=table_name,
            con=dwh_engine,
            schema="public",
            if_exists="append",
            index=False
        )
        print(f"Loaded {len(df)} rows into {table_name}")

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
        python_callable=extract_and_load,
    )