use cloud_db.aws_schema;


create or replace stage cloud_db.aws_schema.retail_transaction_stage 
url="s3://online-retail-transaction/output_folder/" 
credentials=(
    aws_key_id='<aws_key_id>'
    aws_secret_key='<aws_secret_key>'
);


create or replace file format my_parquet_format
type = parquet;


-- Create Tables
CREATE OR REPLACE EXTERNAL TABLE transactions (
    invoice STRING AS (Value:invoice::STRING),
    stock_code STRING AS (Value:stock_code::STRING),
    description STRING AS (Value:description::STRING),
    quantity INTEGER AS (Value:quantity::INTEGER),
    invoice_date TIMESTAMP AS (Value:invoice_date::TIMESTAMP),
    price DOUBLE AS (Value:price::DOUBLE),
    customer_id STRING AS (Value:customer_id::STRING),
    country STRING AS (Value:country::STRING)
)
WITH LOCATION = @cloud_db.aws_schema.retail_transaction_stage/transactions
FILE_FORMAT = 'my_parquet_format';

CREATE OR REPLACE EXTERNAL TABLE country (
    customer_id STRING AS (Value:customer_id::STRING),
    country STRING AS (Value:country::STRING)
)
WITH LOCATION = @cloud_db.aws_schema.retail_transaction_stage/country
FILE_FORMAT = 'my_parquet_format';

CREATE OR REPLACE EXTERNAL TABLE product (
    stock_code STRING AS (Value:stock_code::STRING),
    description STRING AS (Value:description::STRING)
)
WITH LOCATION = @cloud_db.aws_schema.retail_transaction_stage/product
FILE_FORMAT = 'my_parquet_format';

CREATE OR REPLACE EXTERNAL TABLE time (
    full_date TIMESTAMP AS (Value:full_date::TIMESTAMP),
    date_key STRING AS (Value:date_key::STRING),
    year INTEGER AS (Value:year::INTEGER),
    quarter INTEGER AS (Value:quarter::INTEGER),
    month INTEGER AS (Value:month::INTEGER),
    month_name STRING AS (Value:month_name::STRING),
    day INTEGER AS (Value:day::INTEGER),
    week_of_year INTEGER AS (Value:week_of_year::INTEGER),
    week_of_month INTEGER AS (Value:week_of_month::INTEGER),
    day_of_week INTEGER AS (Value:day_of_week::INTEGER),
    day_name STRING AS (Value:day_name::STRING),
    is_weekend BOOLEAN AS (Value:is_weekend::BOOLEAN),
    is_holiday BOOLEAN AS (Value:is_holiday::BOOLEAN)
)
WITH LOCATION = @cloud_db.aws_schema.retail_transaction_stage/time
FILE_FORMAT = 'my_parquet_format';




ALTER EXTERNAL TABLE cloud_db.aws_schema.transactions REFRESH;
ALTER EXTERNAL TABLE cloud_db.aws_schema.country REFRESH;
ALTER EXTERNAL TABLE cloud_db.aws_schema.product REFRESH;
ALTER EXTERNAL TABLE cloud_db.aws_schema.time REFRESH;
