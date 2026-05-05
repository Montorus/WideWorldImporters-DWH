import pandas as pd
from sqlalchemy import text
from datetime import datetime, timezone
from airflow.providers.postgres.hooks.postgres import PostgresHook

TABLES = {
    "raw_customers": """
        SELECT
            c.customerid,
            c.personid,
            c.storeid,
            c.territoryid,
            c.accountnumber,
            COALESCE(
                NULLIF(CONCAT_WS(' ', p.firstname, p.middlename, p.lastname), ''),
                s.name,
                c.accountnumber,
                'Customer ' || c.customerid::text
            ) AS customer_name,
            p.firstname AS first_name,
            p.middlename AS middle_name,
            p.lastname AS last_name,
            s.name AS store_name,
            c.modifieddate
        FROM sales.customer c
        LEFT JOIN person.person p
            ON c.personid = p.businessentityid
        LEFT JOIN sales.store s
            ON c.storeid = s.businessentityid
    """,
    "raw_sales_orders": """
        SELECT
            salesorderid,
            revisionnumber,
            orderdate,
            duedate,
            shipdate,
            status,
            onlineorderflag,
            purchaseordernumber,
            accountnumber,
            customerid,
            salespersonid,
            territoryid,
            billtoaddressid,
            shiptoaddressid,
            shipmethodid,
            creditcardid,
            creditcardapprovalcode,
            currencyrateid,
            subtotal,
            taxamt,
            freight,
            totaldue,
            rowguid,
            modifieddate
        FROM sales.salesorderheader
    """,
    "raw_sales_details": """
        SELECT
            salesorderid,
            salesorderdetailid,
            carriertrackingnumber,
            orderqty,
            productid,
            specialofferid,
            unitprice,
            unitpricediscount,
            rowguid,
            modifieddate
        FROM sales.salesorderdetail
    """,
    "raw_employees": """
        SELECT
            businessentityid,
            nationalidnumber,
            loginid,
            organizationnode,
            organizationlevel,
            jobtitle,
            birthdate,
            maritalstatus,
            gender,
            hiredate,
            salariedflag,
            vacationhours,
            sickleavehours,
            currentflag,
            rowguid,
            modifieddate
        FROM humanresources.employee
    """,
    "raw_products": """
        SELECT
            productid,
            name,
            productnumber,
            makeflag,
            finishedgoodsflag,
            color,
            safetystocklevel,
            reorderpoint,
            standardcost,
            listprice,
            size,
            sizeunitmeasurecode,
            weightunitmeasurecode,
            weight,
            daystomanufacture,
            productline,
            class,
            style,
            productsubcategoryid,
            productmodelid,
            sellstartdate,
            sellenddate,
            discontinueddate,
            rowguid,
            modifieddate
        FROM production.product
    """,
}

def extract_and_load(source_conn_id: str, dwh_conn_id: str, dag_name: str = None):
    source_engine = PostgresHook(postgres_conn_id=source_conn_id).get_sqlalchemy_engine()
    dwh_engine = PostgresHook(postgres_conn_id=dwh_conn_id).get_sqlalchemy_engine()

    try:
        for table_name, query in TABLES.items():
            started_at = datetime.now(timezone.utc)
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
                    finished_at = datetime.now(timezone.utc)
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
    finally:
        source_engine.dispose()
        dwh_engine.dispose()
