# Source Database Operations

This project uses Docker databases as the canonical runtime sources. Windows SQL Server instances may exist on the same PC, but Airflow does not use them unless `docker/docker-compose.yml` is changed.

## Canonical Targets

| Purpose | Runtime target | Used by | Notes |
|---|---|---|---|
| WideWorldImporters source | Docker service `mssql`, container `mssql_source`, database `WideWorldImporters` | `etl_mssql_customers` | Connect from SSMS with `localhost,1433` |
| AdventureWorks source | Docker service `postgres_source`, database from `POSTGRES_SOURCE_DB` | `etl_postgresql_adventureworks` | Loaded from CSV files |
| Data warehouse | Docker service `postgres_dwh`, database from `POSTGRES_DWH_DB` | Airflow and dbt | Stores RAW, staging, marts, and `pipeline_runs` |

Do not use `.` or plain `localhost` in SSMS when you want this project's MSSQL database. Use `localhost,1433` so the target is explicit.

## Current Local SQL Server Instances

The PC can have several SQL Server instances:

| Instance | Typical connection | Project role |
|---|---|---|
| Docker MSSQL | `localhost,1433` from Windows, `mssql_source:1433` from containers | Project source |
| Windows default SQL Server | `.` or `localhost` | Not used by this project |
| Windows SQL Express | `.\SQLEXPRESS` | Not used by this project |

If a backup or restore is done against the wrong instance, Airflow will still fail because Airflow talks to Docker `mssql_source`, not the Windows services.

## MSSQL Checks

From `docker/`:

```powershell
docker compose exec mssql sh -c '/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -C -Q "select name from sys.databases order by name"'
```

Expected project database:

```text
WideWorldImporters
```

Check the table used by Airflow:

```powershell
docker compose exec mssql sh -c '/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -C -d WideWorldImporters -Q "select count(*) as customer_count from Sales.Customers"'
```

Check the same route Airflow uses:

```powershell
docker compose exec airflow sh -c 'python -c "from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook; e=MsSqlHook(mssql_conn_id=\"mssql_source\").get_sqlalchemy_engine(); c=e.connect(); print(c.exec_driver_sql(\"select count(*) from Sales.Customers\").fetchall()); c.close(); e.dispose()"'
```

## MSSQL Backup

Preferred SSMS target:

```text
localhost,1433
```

Backup destination inside Docker:

```text
/var/opt/mssql/data/WideWorldImporters.bak
```

Terminal check:

```powershell
docker compose exec mssql sh -c 'ls -lh /var/opt/mssql/data/WideWorldImporters.bak'
```

The repository ignores `*.bak`; backup files should stay outside Git.

## MSSQL Restore

Copy a backup from Windows to Docker when needed:

```powershell
docker compose cp C:\path\to\WideWorldImporters.bak mssql:/var/opt/mssql/data/WideWorldImporters.bak
```

Inspect logical file names:

```powershell
docker compose exec mssql sh -c '/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -C -Q "RESTORE FILELISTONLY FROM DISK = ''/var/opt/mssql/data/WideWorldImporters.bak''"'
```

Restore with the logical names returned by `RESTORE FILELISTONLY`:

```powershell
docker compose exec mssql sh -c '/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -C -Q "RESTORE DATABASE WideWorldImporters FROM DISK = ''/var/opt/mssql/data/WideWorldImporters.bak'' WITH REPLACE, MOVE ''WideWorldImporters'' TO ''/var/opt/mssql/data/WideWorldImporters.mdf'', MOVE ''WideWorldImporters_log'' TO ''/var/opt/mssql/data/WideWorldImporters_log.ldf''"'
```

If the logical names are different, adjust the two `MOVE` entries.

## AdventureWorks CSV Import

The project does not require the full 68-table AdventureWorks install. It requires these source tables:

```text
Person.csv
Store.csv
Customer.csv
SalesOrderHeader.csv
SalesOrderDetail.csv
Product.csv
Employee.csv
```

The reusable import contract is:

```text
sql/queries/import_adventureworks_required.sql
```

Create a converted temporary working copy of the CSV directory:

```powershell
.\.venv\Scripts\python.exe scripts\convert_adventureworks_csv.py C:\path\to\AdventureWorks .tmp\AdventureWorks --force
```

Then copy the converted folder and import script into the container:

```powershell
docker compose cp ..\.tmp\AdventureWorks postgres_source:/tmp/AdventureWorks
docker compose cp ..\sql\queries\import_adventureworks_required.sql postgres_source:/tmp/AdventureWorks/import_adventureworks_required.sql
```

Run the import from the CSV directory:

```powershell
docker compose exec postgres_source sh -c 'cd /tmp/AdventureWorks && psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -f import_adventureworks_required.sql'
```

Expected imported counts from the current dataset:

| Table | Expected rows |
|---|---:|
| `person.person` | 19972 |
| `sales.store` | 701 |
| `sales.customer` | 19820 |
| `sales.salesorderheader` | 31465 |
| `sales.salesorderdetail` | 121317 |
| `production.product` | 504 |
| `humanresources.employee` | 290 |

## AdventureWorks Checks

From `docker/`:

```powershell
docker compose exec postgres_source sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "select ''sales.customer'' as table_name, count(*) from sales.customer union all select ''sales.salesorderheader'', count(*) from sales.salesorderheader union all select ''sales.salesorderdetail'', count(*) from sales.salesorderdetail union all select ''production.product'', count(*) from production.product union all select ''humanresources.employee'', count(*) from humanresources.employee order by table_name;"'
```

Check the Airflow ETL route:

```powershell
docker compose exec airflow sh -c 'python -c "from etl.extract.postgresql import extract_and_load; extract_and_load(\"postgres_source\", \"postgres_dwh\", \"etl_postgresql_adventureworks_manual_check\")"'
```

## End-To-End Validation

After source restore/import:

```powershell
docker compose exec airflow python -m unittest discover -s /opt/airflow/tests -p "test_*.py"
```

From `dbt/`:

```powershell
..\.venv\Scripts\dbt.exe run --threads 1
..\.venv\Scripts\dbt.exe test --threads 1
```

Successful baseline after the current AdventureWorks import:

```text
dbt run:  PASS=10
dbt test: PASS=50
fact_sales rows: 121317
```
