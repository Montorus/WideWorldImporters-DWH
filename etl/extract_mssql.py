import pandas as pd
from sqlalchemy import create_engine

# MSSQL connection
mssql_engine = create_engine(
    "mssql+pymssql://sa:StrongPass123!@127.0.0.1:1433/WideWorldImporters"
)

# PostgreSQL DWH connection
dwh_engine = create_engine(
    "postgresql://dwh_user:dwh123@127.0.0.1:5434/warehouse_db"
)

# Extract data from MSSQL
df = pd.read_sql("""
    SELECT
        CustomerID,
        CustomerName,
        CreditLimit,
        IsStatementSent,
        IsOnCreditHold
    FROM Sales.Customers
""", mssql_engine)

print(f"Rows extracted: {len(df)}")
print(df.head())

# Load into PostgreSQL DWH
df.to_sql(
    name="raw_customers",
    con=dwh_engine,
    schema="public",
    if_exists="replace",
    index=False
)

print("Loaded into DWH successfully!")