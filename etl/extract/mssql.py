import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

mssql_engine = create_engine(
    f"mssql+pymssql://sa:{os.getenv('MSSQL_SA_PASSWORD')}@127.0.0.1:1433/WideWorldImporters"
)

dwh_engine = create_engine(
    f"postgresql://{os.getenv('POSTGRES_DWH_USER')}:{os.getenv('POSTGRES_DWH_PASSWORD')}@127.0.0.1:5434/{os.getenv('POSTGRES_DWH_DB')}"
)

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

df.to_sql(
    name="raw_customers",
    con=dwh_engine,
    schema="public",
    if_exists="replace",
    index=False
)

print("Loaded into DWH successfully!")