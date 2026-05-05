# Documentation Index

This directory contains supporting documentation and reporting artifacts for the WideWorldImporters Data Warehouse project.

Start with the root [README.md](../README.md) for setup, architecture, and run commands. Use this directory for deeper operational and ETL-specific context.

## Contents

| Path | Purpose |
|---|---|
| [`etl/pipeline_flow.md`](etl/pipeline_flow.md) | Technical ETL flow from source systems to RAW, staging, marts, tests, and Power BI |
| [`operations/source_databases.md`](operations/source_databases.md) | Canonical source DB targets, backup/restore rules, CSV import steps, and validation checks |
| `WideWorldImporters-PowerBI.pbix` | Power BI report artifact connected to the warehouse mart layer |

## Runtime Areas

| Area | Location | Notes |
|---|---|---|
| Docker runtime | [`../docker/docker-compose.yml`](../docker/docker-compose.yml) | Defines MSSQL, PostgreSQL source, PostgreSQL DWH, and Airflow services |
| Airflow DAGs | [`../airflow/dags/`](../airflow/dags) | Defines `etl_mssql_customers` and `etl_postgresql_adventureworks` |
| Extract code | [`../etl/extract/`](../etl/extract) | Contains source-specific Python extract/load helpers |
| DWH initialization | [`../sql/queries/init_warehouse.sql`](../sql/queries/init_warehouse.sql) | Creates RAW tables and `pipeline_runs` |
| dbt project | [`../dbt/`](../dbt) | Builds staging views, mart tables, and data tests |
| Airflow import tests | [`../tests/test_airflow_dags.py`](../tests/test_airflow_dags.py) | Verifies DAG import and expected task registration |
| CI workflow | [`../.github/workflows/dbt_test.yml`](../.github/workflows/dbt_test.yml) | Runs Airflow import tests, `dbt parse`, `dbt run`, and `dbt test` |

## Key Model Boundaries

| Boundary | Decision |
|---|---|
| AdventureWorks sales mart | `fact_sales` and reporting dimensions use AdventureWorks keys |
| WideWorldImporters customers | Stored separately in `raw_wwi_customers`; not joined to `fact_sales` |
| Customer dimension | Built from AdventureWorks `raw_customers` so `fact_sales.customer_id` has a valid dimension |
| Dimension history | Current-state only; SCD Type 2 is not implemented yet |

## Common Commands

From the repository root:

```powershell
docker compose -f docker\docker-compose.yml up -d --build
docker compose -f docker\docker-compose.yml ps
```

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

## Operational Notes

- Airflow connection IDs are provided through `AIRFLOW_CONN_*` environment variables in `docker/docker-compose.yml`.
- `pipeline_runs` stores DAG execution audit metadata per loaded RAW table.
- Generated runtime logs should not be treated as documentation or source code.
