import pandas as pd
from sqlalchemy import create_engine

# MSSQL jungtis
mssql_engine = create_engine(
    "mssql+pymssql://sa:StrongPass123!@127.0.0.1:1433/WideWorldImporters"
)

# PostgreSQL DWH jungtis
dwh_engine = create_engine(
    "postgresql://dwh_user:dwh123@127.0.0.1:5434/warehouse_db"
)

# Ištraukti duomenis iš MSSQL
df = pd.read_sql("""
    SELECT
        CustomerID,
        CustomerName,
        CreditLimit,
        IsStatementSent,
        IsOnCreditHold
    FROM Sales.Customers
""", mssql_engine)

print(f"Ištraukta eilučių: {len(df)}")
print(df.head())

# Įkelti į PostgreSQL DWH
df.to_sql(
    name="raw_customers",
    con=dwh_engine,
    schema="public",
    if_exists="replace",
    index=False
)

print("Įkelta į DWH sėkmingai!")