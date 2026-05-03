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
    mssql_engine = create_engine(
        "mssql+pymssql://sa:StrongPass123!@mssql_source:1433/WideWorldImporters"
    )
    dwh_engine = create_engine(
        "postgresql://dwh_user:dwh123@postgres_dwh:5432/warehouse_db"
    )

    started_at = datetime.utcnow()
    rows_loaded = 0
    status = "success"
    error_message = None

    try:
        df = pd.read_sql("""
            SELECT
                CustomerID,
                CustomerName,
                CustomerCategoryID,
                CreditLimit,
                AccountOpenedDate,
                StandardDiscountPercentage,
                IsStatementSent,
                IsOnCreditHold,
                PaymentDays,
                PhoneNumber,
                WebsiteURL,
                DeliveryAddressLine1,
                DeliveryPostalCode
            FROM Sales.Customers
        """, mssql_engine)

        rows_loaded = len(df)
        print(f"Extracted rows: {rows_loaded}")

        with dwh_engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE public.raw_customers"))
            conn.commit()

        df.to_sql(
            name="raw_customers",
            con=dwh_engine,
            schema="public",
            if_exists="append",
            index=False
        )

        print("Loaded into DWH successfully!")

    except Exception as e:
        status = "failed"
        error_message = str(e)
        raise

    finally:
        finished_at = datetime.utcnow()
        with dwh_engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO public.pipeline_runs
                    (dag_name, table_name, rows_loaded, started_at, finished_at, status, error_message)
                VALUES
                    (:dag_name, :table_name, :rows_loaded, :started_at, :finished_at, :status, :error_message)
            """), {
                "dag_name": "etl_mssql_customers",
                "table_name": "raw_customers",
                "rows_loaded": rows_loaded,
                "started_at": started_at,
                "finished_at": finished_at,
                "status": status,
                "error_message": error_message
            })
            conn.commit()

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
        python_callable=extract_and_load,
    )