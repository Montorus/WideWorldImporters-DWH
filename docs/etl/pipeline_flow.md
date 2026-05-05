# ETL Pipeline Flow

This document describes the technical data flow from source systems into the PostgreSQL warehouse and Power BI reporting layer.

## 1. Source Systems

| Source System | Runtime Service | Database Type | Used For |
|---|---|---|---|
| WideWorldImporters | `mssql_source` | MSSQL | Separate customer archive extract |
| AdventureWorks | `postgres_source` | PostgreSQL | Sales mart source data |
| Data Warehouse | `postgres_dwh` | PostgreSQL | RAW tables, dbt staging views, dbt mart tables |

## 2. Airflow Orchestration

Airflow runs two independent DAGs on a 15-minute schedule with retry handling.

| DAG ID | DAG File | Python Callable | Source Connection | Target Connection |
|---|---|---|---|---|
| `etl_mssql_customers` | `airflow/dags/etl_mssql_dag.py` | `extract_and_load_customers` | `mssql_source` | `postgres_dwh` |
| `etl_postgresql_adventureworks` | `airflow/dags/etl_postgresql_dag.py` | `extract_and_load_adventureworks` | `postgres_source` | `postgres_dwh` |

The DAG files only orchestrate work. Source-specific extraction and loading logic lives under `etl/extract/`.

## 3. Airflow Connections

| Connection ID | Docker Environment Variable | Target |
|---|---|---|
| `mssql_source` | `AIRFLOW_CONN_MSSQL_SOURCE` | `mssql_source:1433/WideWorldImporters` |
| `postgres_source` | `AIRFLOW_CONN_POSTGRES_SOURCE` | `postgres_source:5432/${POSTGRES_SOURCE_DB}` |
| `postgres_dwh` | `AIRFLOW_CONN_POSTGRES_DWH` | `postgres_dwh:5432/${POSTGRES_DWH_DB}` |

## 4. RAW Load Mapping

Airflow truncates and reloads the target RAW table before each append load.

| Extract Module | Source Query/Table | Target RAW Table | Mart Usage |
|---|---|---|---|
| `etl/extract/mssql.py` | `Sales.Customers` | `raw_wwi_customers` | Not joined to `fact_sales`; retained as separate source extract |
| `etl/extract/postgresql.py` | `sales.customer` enriched with `person.person` and `sales.store` | `raw_customers` | Feeds `stg_customers` and `dim_customers` |
| `etl/extract/postgresql.py` | `sales.salesorderheader` | `raw_sales_orders` | Feeds `stg_sales_orders` and `fact_sales` |
| `etl/extract/postgresql.py` | `sales.salesorderdetail` | `raw_sales_details` | Feeds `stg_sales_details` and `fact_sales` |
| `etl/extract/postgresql.py` | `production.product` | `raw_products` | Feeds `stg_products` and `dim_products` |
| `etl/extract/postgresql.py` | `humanresources.employee` | `raw_employees` | Feeds `stg_employees` and `dim_employees` |

The target RAW schema is created by `sql/queries/init_warehouse.sql`.

## 5. Pipeline Audit

Each load writes one row into `pipeline_runs` when `dag_name` is provided.

| Column | Meaning |
|---|---|
| `dag_name` | DAG or pipeline identifier |
| `table_name` | Loaded RAW table |
| `rows_loaded` | Number of rows extracted into the DataFrame |
| `started_at` | UTC start timestamp |
| `finished_at` | UTC finish timestamp |
| `status` | `success` or `failed` |
| `error_message` | Exception text for failed loads |

## 6. dbt Staging Layer

Staging models are materialized as views. They normalize names and filter out null source keys.

| Staging Model | RAW Input | Primary Key Tested |
|---|---|---|
| `stg_customers` | `raw_customers` | `customer_id` |
| `stg_sales_orders` | `raw_sales_orders` | `sales_order_id` |
| `stg_sales_details` | `raw_sales_details` | `sales_order_detail_id` |
| `stg_products` | `raw_products` | `product_id` |
| `stg_employees` | `raw_employees` | `employee_id` |

## 7. dbt Mart Layer

Mart models are materialized as tables for reporting performance.

| Mart Model | Type | Inputs | Purpose |
|---|---|---|---|
| `dim_customers` | Dimension | `stg_customers` | AdventureWorks customer dimension aligned with `fact_sales.customer_id` |
| `dim_products` | Dimension | `stg_products` | Current-state product attributes |
| `dim_employees` | Dimension | `stg_employees` | Current-state employee attributes |
| `dim_date` | Dimension | Generated date spine | Calendar attributes from 2010-01-01 to 2030-12-31 |
| `fact_sales` | Fact | `stg_sales_orders`, `stg_sales_details` | Sales line facts and monetary measures |

The dimensions are current-state tables. They do not preserve historical attribute versions yet.

## 8. Star Schema Relationships

| Fact Column | Dimension | Relationship |
|---|---|---|
| `fact_sales.customer_id` | `dim_customers` | `customer_id` |
| `fact_sales.product_id` | `dim_products` | `product_id` |
| `fact_sales.employee_id` | `dim_employees` | `employee_id` |
| `fact_sales.date_id` | `dim_date` | `date_id` |

dbt relationship tests validate these joins.

## 9. Data Quality

The dbt project defines tests for:

| Test Type | Coverage |
|---|---|
| Source `not_null` | RAW source key columns |
| Staging `not_null` | Required staging keys and measures |
| Staging `unique` | Staging entity keys |
| Mart `not_null` | Dimension keys, fact keys, and core measures |
| Mart `unique` | Dimension keys and `sales_order_detail_id` |
| Mart `relationships` | Fact-to-dimension integrity |

## 10. BI Consumption

Power BI should consume the dbt mart layer:

| Power BI Input | Recommended Use |
|---|---|
| `fact_sales` | Central fact table for sales measures |
| `dim_customers` | Customer slicing and filtering |
| `dim_products` | Product slicing and filtering |
| `dim_employees` | Salesperson/employee slicing and filtering |
| `dim_date` | Calendar filtering, period grouping, time intelligence |

## 11. Validation Commands

From `dbt/`:

```powershell
..\.venv\Scripts\dbt.exe parse
..\.venv\Scripts\dbt.exe run
..\.venv\Scripts\dbt.exe test
```

From `docker/`:

```powershell
docker compose exec airflow python -m unittest discover -s /opt/airflow/tests -p "test_*.py"
```

GitHub Actions runs equivalent checks for pull requests and pushes to `main`.

## Follow-Up Options

| Future Work | Reason |
|---|---|
| Add SCD Type 2 with dbt snapshots | Preserve historical dimension attribute changes |
| Add freshness checks | Detect stale source loads |
| Add richer Power BI documentation | Describe report pages, measures, and refresh expectations |
