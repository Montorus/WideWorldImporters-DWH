from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta, timezone
import pandas as pd
from sqlalchemy import text

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def extract_and_load():
    mssql_engine = MsSqlHook(mssql_conn_id="mssql_source").get_sqlalchemy_engine()
    dwh_engine = PostgresHook(postgres_conn_id="postgres_dwh").get_sqlalchemy_engine()

    started_at = datetime.now(timezone.utc)
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
        finished_at = datetime.now(timezone.utc)
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
        mssql_engine.dispose()
        dwh_engine.dispose()

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