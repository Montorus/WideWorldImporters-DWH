-- AdventureWorks PostgreSQL source import for this project's ETL.
--
-- This script imports only the source tables used by etl.extract.postgresql.
-- Run it from a directory that contains the converted AdventureWorks CSV files:
-- Person.csv, Store.csv, Customer.csv, SalesOrderHeader.csv,
-- SalesOrderDetail.csv, Product.csv, Employee.csv.

DROP SCHEMA IF EXISTS sales CASCADE;
DROP SCHEMA IF EXISTS person CASCADE;
DROP SCHEMA IF EXISTS production CASCADE;
DROP SCHEMA IF EXISTS humanresources CASCADE;
DROP SCHEMA IF EXISTS purchasing CASCADE;
DROP SCHEMA IF EXISTS pe CASCADE;
DROP SCHEMA IF EXISTS hr CASCADE;
DROP SCHEMA IF EXISTS pr CASCADE;
DROP SCHEMA IF EXISTS pu CASCADE;
DROP SCHEMA IF EXISTS sa CASCADE;

CREATE SCHEMA sales;
CREATE SCHEMA person;
CREATE SCHEMA production;
CREATE SCHEMA humanresources;

CREATE TABLE person.person (
    businessentityid BIGINT,
    persontype TEXT,
    namestyle BOOLEAN,
    title TEXT,
    firstname TEXT,
    middlename TEXT,
    lastname TEXT,
    suffix TEXT,
    emailpromotion BIGINT,
    additionalcontactinfo XML,
    demographics XML,
    rowguid UUID,
    modifieddate TIMESTAMP
);

CREATE TABLE sales.store (
    businessentityid BIGINT,
    name TEXT,
    salespersonid BIGINT,
    demographics XML,
    rowguid UUID,
    modifieddate TIMESTAMP
);

CREATE TABLE sales.customer (
    customerid BIGINT,
    personid BIGINT,
    storeid BIGINT,
    territoryid BIGINT,
    accountnumber TEXT,
    rowguid UUID,
    modifieddate TIMESTAMP
);

CREATE TABLE sales.salesorderheader (
    salesorderid BIGINT,
    revisionnumber BIGINT,
    orderdate TIMESTAMP,
    duedate TIMESTAMP,
    shipdate TIMESTAMP,
    status BIGINT,
    onlineorderflag BOOLEAN,
    salesordernumber TEXT,
    purchaseordernumber TEXT,
    accountnumber TEXT,
    customerid BIGINT,
    salespersonid BIGINT,
    territoryid BIGINT,
    billtoaddressid BIGINT,
    shiptoaddressid BIGINT,
    shipmethodid BIGINT,
    creditcardid BIGINT,
    creditcardapprovalcode TEXT,
    currencyrateid BIGINT,
    subtotal DOUBLE PRECISION,
    taxamt DOUBLE PRECISION,
    freight DOUBLE PRECISION,
    totaldue DOUBLE PRECISION,
    comment TEXT,
    rowguid UUID,
    modifieddate TIMESTAMP
);

CREATE TABLE sales.salesorderdetail (
    salesorderid BIGINT,
    salesorderdetailid BIGINT,
    carriertrackingnumber TEXT,
    orderqty BIGINT,
    productid BIGINT,
    specialofferid BIGINT,
    unitprice DOUBLE PRECISION,
    unitpricediscount DOUBLE PRECISION,
    linetotal DOUBLE PRECISION,
    rowguid UUID,
    modifieddate TIMESTAMP
);

CREATE TABLE production.product (
    productid BIGINT,
    name TEXT,
    productnumber TEXT,
    makeflag BOOLEAN,
    finishedgoodsflag BOOLEAN,
    color TEXT,
    safetystocklevel BIGINT,
    reorderpoint BIGINT,
    standardcost DOUBLE PRECISION,
    listprice DOUBLE PRECISION,
    size TEXT,
    sizeunitmeasurecode TEXT,
    weightunitmeasurecode TEXT,
    weight DOUBLE PRECISION,
    daystomanufacture BIGINT,
    productline TEXT,
    class TEXT,
    style TEXT,
    productsubcategoryid BIGINT,
    productmodelid BIGINT,
    sellstartdate TIMESTAMP,
    sellenddate TIMESTAMP,
    discontinueddate TIMESTAMP,
    rowguid UUID,
    modifieddate TIMESTAMP
);

CREATE TABLE humanresources.employee (
    businessentityid BIGINT,
    nationalidnumber TEXT,
    loginid TEXT,
    organizationnode TEXT,
    organizationlevel BIGINT,
    jobtitle TEXT,
    birthdate DATE,
    maritalstatus TEXT,
    gender TEXT,
    hiredate DATE,
    salariedflag BOOLEAN,
    vacationhours BIGINT,
    sickleavehours BIGINT,
    currentflag BOOLEAN,
    rowguid UUID,
    modifieddate TIMESTAMP
);

\copy person.person FROM 'Person.csv' DELIMITER E'\t' CSV;
\copy sales.store FROM 'Store.csv' DELIMITER E'\t' CSV;
\copy sales.customer FROM 'Customer.csv' DELIMITER E'\t' CSV;
\copy sales.salesorderheader FROM 'SalesOrderHeader.csv' DELIMITER E'\t' CSV;
\copy sales.salesorderdetail FROM 'SalesOrderDetail.csv' DELIMITER E'\t' CSV;
\copy production.product FROM 'Product.csv' DELIMITER E'\t' CSV;
\copy humanresources.employee FROM 'Employee.csv' DELIMITER E'\t' CSV;
