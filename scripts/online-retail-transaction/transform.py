import sys


from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *

# Create a Spark session
spark = SparkSession \
        .builder \
        .appName("airflow_with_emr") \
        .getOrCreate()

# Function to read from S3, transform using Spark, and save as Parquet
def main():
    print("LOGGING: In main function.")
    s3_location = "s3://online-retail-transaction/input_folder/"

    # Define the schema explicitly for the Spark DataFrame
    schema = StructType([
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", StringType(), True),  # Keep as string for now
        StructField("UnitPrice", DoubleType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Country", StringType(), True)
    ])

    # Read the CSV file directly from S3 into a Spark DataFrame
    print("LOGGING: Reading CSV file from S3 into Spark DataFrame.")
    spark_df = spark.read.format("csv") \
        .option("inferSchema","true") \
        .option("header", "true") \
        .schema(schema) \
        .load(s3_location)

    print("LOGGING: Created Spark DataFrame.")

    # Write the Spark DataFrame as a Parquet file back to S3
    output_location = "s3://online-retail-transaction/output_folder/"
    print(f"LOGGING: Saving Spark DataFrame as Parquet to {output_location}")
    spark_df.write.format("parquet").mode('overwrite').save(output_location)

    print("LOGGING: Completed writing to Parquet.")

main()
