import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Tables to extract from AdventureWorks
TABLES = {
    "raw_sales_orders": "SELECT * FROM sales.salesorderheader",
    "raw_sales_details": "SELECT * FROM sales.salesorderdetail",
    "raw_employees":     "SELECT * FROM humanresources.employee",
    "raw_products":      "SELECT * FROM production.product",
}

def extract_and_load(source_host: str, dwh_host: str, dag_name: str = None):
    source_engine = create_engine(
        f"postgresql://postgres:postgres123@{source_host}:5432/source_db"
    )
    dwh_engine = create_engine(
        f"postgresql://dwh_user:dwh123@{dwh_host}:5432/warehouse_db"
    )

    for table_name, query in TABLES.items():
        started_at = datetime.utcnow()
        rows_loaded = 0
        status = "success"
        error_message = None

        try:
            df = pd.read_sql(query, source_engine)
            rows_loaded = len(df)

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
            print(f"Loaded {rows_loaded} rows into {table_name}")

        except Exception as e:
            status = "failed"
            error_message = str(e)
            raise

        finally:
            if dag_name:
                finished_at = datetime.utcnow()
                with dwh_engine.connect() as conn:
                    conn.execute(text("""
                        INSERT INTO public.pipeline_runs
                            (dag_name, table_name, rows_loaded, started_at, finished_at, status, error_message)
                        VALUES
                            (:dag_name, :table_name, :rows_loaded, :started_at, :finished_at, :status, :error_message)
                    """), {
                        "dag_name": dag_name,
                        "table_name": table_name,
                        "rows_loaded": rows_loaded,
                        "started_at": started_at,
                        "finished_at": finished_at,
                        "status": status,
                        "error_message": error_message
                    })
                    conn.commit()


if __name__ == "__main__":
    # Local execution for testing
    extract_and_load(
        source_host="127.0.0.1",
        dwh_host="127.0.0.1:5434".split(":")[0],
        dag_name=None
    )