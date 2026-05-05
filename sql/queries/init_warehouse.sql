-- Warehouse initialization script.
-- Creates RAW layer tables used by Airflow and metadata tables used for pipeline monitoring.

CREATE TABLE IF NOT EXISTS public.pipeline_runs (
    id              SERIAL PRIMARY KEY,
    dag_name        TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    rows_loaded     INTEGER,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    status          TEXT,
    error_message   TEXT
);

CREATE TABLE IF NOT EXISTS public.raw_customers (
    customerid                      BIGINT,
    personid                        DOUBLE PRECISION,
    storeid                         DOUBLE PRECISION,
    territoryid                     BIGINT,
    accountnumber                   TEXT,
    customer_name                   TEXT,
    first_name                      TEXT,
    middle_name                     TEXT,
    last_name                       TEXT,
    store_name                      TEXT,
    modifieddate                    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.raw_wwi_customers (
    "CustomerID"                    BIGINT,
    "CustomerName"                  TEXT,
    "CustomerCategoryID"            BIGINT,
    "CreditLimit"                   DOUBLE PRECISION,
    "AccountOpenedDate"             DATE,
    "StandardDiscountPercentage"    DOUBLE PRECISION,
    "IsStatementSent"               BOOLEAN,
    "IsOnCreditHold"                BOOLEAN,
    "PaymentDays"                   BIGINT,
    "PhoneNumber"                   TEXT,
    "WebsiteURL"                    TEXT,
    "DeliveryAddressLine1"          TEXT,
    "DeliveryPostalCode"            TEXT
);

CREATE TABLE IF NOT EXISTS public.raw_sales_orders (
    salesorderid                BIGINT,
    revisionnumber              BIGINT,
    orderdate                   TIMESTAMP,
    duedate                     TIMESTAMP,
    shipdate                    TIMESTAMP,
    status                      BIGINT,
    onlineorderflag             BOOLEAN,
    purchaseordernumber         TEXT,
    accountnumber               TEXT,
    customerid                  BIGINT,
    salespersonid               DOUBLE PRECISION,
    territoryid                 BIGINT,
    billtoaddressid             BIGINT,
    shiptoaddressid             BIGINT,
    shipmethodid                BIGINT,
    creditcardid                DOUBLE PRECISION,
    creditcardapprovalcode      TEXT,
    currencyrateid              DOUBLE PRECISION,
    subtotal                    DOUBLE PRECISION,
    taxamt                      DOUBLE PRECISION,
    freight                     DOUBLE PRECISION,
    totaldue                    DOUBLE PRECISION,
    rowguid                     TEXT,
    modifieddate                TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.raw_sales_details (
    salesorderid                BIGINT,
    salesorderdetailid          BIGINT,
    carriertrackingnumber       TEXT,
    orderqty                    BIGINT,
    productid                   BIGINT,
    specialofferid              BIGINT,
    unitprice                   DOUBLE PRECISION,
    unitpricediscount           DOUBLE PRECISION,
    rowguid                     TEXT,
    modifieddate                TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.raw_products (
    productid                   BIGINT,
    name                        TEXT,
    productnumber               TEXT,
    makeflag                    BOOLEAN,
    finishedgoodsflag           BOOLEAN,
    color                       TEXT,
    safetystocklevel            BIGINT,
    reorderpoint                BIGINT,
    standardcost                DOUBLE PRECISION,
    listprice                   DOUBLE PRECISION,
    size                        TEXT,
    sizeunitmeasurecode         TEXT,
    weightunitmeasurecode       TEXT,
    weight                      DOUBLE PRECISION,
    daystomanufacture           BIGINT,
    productline                 TEXT,
    class                       TEXT,
    style                       TEXT,
    productsubcategoryid        DOUBLE PRECISION,
    productmodelid              DOUBLE PRECISION,
    sellstartdate               TIMESTAMP,
    sellenddate                 TIMESTAMP,
    discontinueddate            TIMESTAMP,
    rowguid                     TEXT,
    modifieddate                TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.raw_employees (
    businessentityid            BIGINT,
    nationalidnumber            TEXT,
    loginid                     TEXT,
    organizationnode            TEXT,
    organizationlevel           DOUBLE PRECISION,
    jobtitle                    TEXT,
    birthdate                   DATE,
    maritalstatus               TEXT,
    gender                      TEXT,
    hiredate                    DATE,
    salariedflag                BOOLEAN,
    vacationhours               BIGINT,
    sickleavehours              BIGINT,
    currentflag                 BOOLEAN,
    rowguid                     TEXT,
    modifieddate                TIMESTAMP
);
