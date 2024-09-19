import sys
import pandas as pd

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *


spark = SparkSession \
        .builder \
        .appName("airflow_with_emr") \
        .getOrCreate()
        


def main():
    print("LOGGING: In main function.")
    s3_location="s3://online-retail-transaction/input_folder/";

    file_location = "s3://online-retail-transaction/input_folder/sample_data.xlsx"
    df = pd.read_excel(file_location)
    print("LOGGING: Created pandas DF.")
    # Define the schema explicitly
    schema = StructType([
        StructField("Invoice", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", StringType(), True),  # Keep as string, you can convert to date later
        StructField("Price", DoubleType(), True),
        StructField("Customer ID", StringType(), True),
        StructField("Country", StringType(), True)
    ])

    # Convert Pandas DataFrame to PySpark DataFrame with the defined schema
    spark_df = spark.createDataFrame(df, schema=schema)
    print("LOGGING: Created Spark DF.")
    spark_df.write.format("parquet").mode('overwrite').save("s3://online-retail-transaction/output_folder/")

main()


