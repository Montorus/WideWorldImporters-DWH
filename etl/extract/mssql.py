from datetime import datetime, timezone

import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text


CUSTOMERS_QUERY = """
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
"""


def extract_and_load(source_conn_id: str, dwh_conn_id: str, dag_name: str = None):
    mssql_engine = MsSqlHook(mssql_conn_id=source_conn_id).get_sqlalchemy_engine()
    dwh_engine = PostgresHook(postgres_conn_id=dwh_conn_id).get_sqlalchemy_engine()

    started_at = datetime.now(timezone.utc)
    rows_loaded = 0
    status = "success"
    error_message = None

    try:
        df = pd.read_sql(CUSTOMERS_QUERY, mssql_engine)
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
            index=False,
        )

        print("Loaded into DWH successfully!")

    except Exception as e:
        status = "failed"
        error_message = str(e)
        raise

    finally:
        if dag_name:
            finished_at = datetime.now(timezone.utc)
            with dwh_engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO public.pipeline_runs
                        (dag_name, table_name, rows_loaded, started_at, finished_at, status, error_message)
                    VALUES
                        (:dag_name, :table_name, :rows_loaded, :started_at, :finished_at, :status, :error_message)
                """), {
                    "dag_name": dag_name,
                    "table_name": "raw_customers",
                    "rows_loaded": rows_loaded,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "status": status,
                    "error_message": error_message,
                })
                conn.commit()

        mssql_engine.dispose()
        dwh_engine.dispose()
