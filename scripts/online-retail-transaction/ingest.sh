#!/bin/bash

# Download the dataset using wget
wget https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/refs/heads/master/data/retail-data/all/online-retail-dataset.csv -O online_retail_dataset.csv

# Check if the download was successful
if [ $? -eq 0 ]; then
    echo "Download successful"
else
    echo "Download failed"
    exit 1
fi

# Upload the downloaded CSV file to the specified S3 bucket using AWS CLI
aws s3 cp online_retail_dataset.csv s3://online-retail-transaction/input_folder/online_retail_dataset.csv

# Check if the S3 upload was successful
if [ $? -eq 0 ]; then
    echo "Upload to S3 successful"
else
    echo "Upload to S3 failed"
    exit 1
fi
