import sys


from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp

# Create a Spark session
spark = SparkSession \
        .builder \
        .appName("online_retail_dataset") \
        .getOrCreate()

# Function to read from S3, transform using Spark, and save as Parquet
def main():
    s3_location = "s3://online-retail-transaction/input_folder/"

    # Define the schema explicitly for the Spark DataFrame
    schema = StructType([
        StructField("invoice", StringType(), True),
        StructField("stock_code", StringType(), True),
        StructField("description", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("invoice_date", StringType(), True),  # Keep as string, you can convert to date later
        StructField("price", DoubleType(), True),
        StructField("customer_id", StringType(), True),
        StructField("country", StringType(), True)
    ])

    # Read the CSV file directly from S3 into a Spark DataFrame
    spark_df = spark.read.format("csv") \
        .option("inferSchema","true") \
        .option("header", "true") \
        .schema(schema) \
        .load(s3_location)

    # removing all the rows with empty customer_id
    trans_df = spark_df.filter(~col('customer_id').isNull())
    # format the date to timestamp format
    trans_df = trans_df.withColumn('invoice_date', to_timestamp(col('invoice_date'), "M/d/yyyy H:mm"))

    # country table
    country_df = trans_df.select(["customer_id", "country"]).distinct()

    # product table
    product_df = trans_df.select(["stock_code", "description"]).distinct()

    trans_df.createOrReplaceTempView("transaction")

    time_df = spark.sql('''
        WITH formatted_data AS (
            SELECT to_timestamp(invoice_date, "M/d/yyyy H:mm") AS parsed_date
            FROM transaction
        )
        SELECT 
            -- Full date (transaction date)
            parsed_date AS FullDate,
            
            -- Surrogate key in the format YYYYMMDD
            date_format(parsed_date, "yyyyMMdd") AS DateKey,
            
            -- Year
            year(parsed_date) AS Year,
            
            -- Quarter of the year (1-4)
            quarter(parsed_date) AS Quarter,
            
            -- Month of the year (1-12)
            month(parsed_date) AS Month,
            
            -- Name of the month (e.g., September)
            date_format(parsed_date, "MMMM") AS MonthName,
            
            -- Day of the month (1-31)
            dayofmonth(parsed_date) AS Day,
            
            -- Week of the year (1-52/53)
            weekofyear(parsed_date) AS WeekOfYear,
            
            -- Week of the month (1-5)
            ceil(dayofmonth(parsed_date)/7.0) AS WeekOfMonth,
            
            -- Day of the week (1 = Monday, 7 = Sunday)
            dayofweek(parsed_date) AS DayOfWeek,
            
            -- Name of the day (e.g., Monday)
            date_format(parsed_date, "EEEE") AS DayName,
            
            -- IsWeekend (1 = true, 0 = false)
            CASE WHEN dayofweek(parsed_date) IN (1, 7) THEN true ELSE false END AS IsWeekend,
            
        -- IsHoliday: Checking against Canadian holidays
            CASE 
                WHEN date_format(parsed_date, "MM/dd") = '01/01' THEN true -- New Year's Day
                WHEN date_format(parsed_date, "MM/dd") = '07/01' THEN true -- Canada Day
                WHEN date_format(parsed_date, "MM/dd") = '12/25' THEN true -- Christmas Day
                WHEN dayofweek(parsed_date) = 2 AND month(parsed_date) = 10 AND weekofyear(parsed_date) - weekofyear(parsed_date - interval 7 days) = 1 THEN true -- Thanksgiving (2nd Monday of October)
                WHEN dayofweek(parsed_date) = 2 AND month(parsed_date) = 9 AND weekofyear(parsed_date) - weekofyear(parsed_date - interval 7 days) = 1 THEN true -- Labour Day (1st Monday of September)
                ELSE false 
            END AS IsHoliday
            
        FROM formatted_data
    ''')

    # Write the Spark DataFrame as a Parquet file back to S3
    output_location = "s3://online-retail-transaction/output_folder/"
    
    # loading data to S3
    trans_df.write.format("parquet").mode('overwrite').save(f"{output_location}/transactions/")
    country_df.write.format("parquet").mode('overwrite').save(f"{output_location}/country/")
    product_df.write.format("parquet").mode('overwrite').save(f"{output_location}/product/")
    time_df.write.format("parquet").mode('overwrite').save(f"{output_location}/time/")

    print("LOGGING: Completed writing to Parquet.")

main()
