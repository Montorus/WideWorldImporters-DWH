import pandas as pd
from sqlalchemy import create_engine

# Connect to MSSQL WideWorldImporters
mssql_engine = create_engine(
    "mssql+pymssql://sa:StrongPass123!@127.0.0.1:1433/WideWorldImporters"
)

# Connect to PostgreSQL DWH
dwh_engine = create_engine(
    "postgresql://dwh_user:dwh123@127.0.0.1:5434/warehouse_db"
)

# Extract customers from WideWorldImporters
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

print(f"Extracted rows: {len(df)}")

# Load into DWH
df.to_sql(
    name="raw_customers",
    con=dwh_engine,
    schema="public",
    if_exists="replace",
    index=False
)

print("Loaded into DWH successfully!")