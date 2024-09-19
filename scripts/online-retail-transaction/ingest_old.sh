#!/bin/bash
# wget - https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip && unzip online+retail+ii.zip && aws s3 cp online_retail_II.xlsx s3://online-retail-transaction/input_folder/online_retail_II.xlsx


# Download the dataset using wget
wget https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip -O online_retail_ii.zip

# Check if the download was successful
if [ $? -eq 0 ]; then
    echo "Download successful"
else
    echo "Download failed"
    exit 1
fi

# Unzip the downloaded file
unzip online_retail_ii.zip

# Check if unzip was successful
if [ $? -eq 0 ]; then
    echo "Unzip successful"
else
    echo "Unzip failed"
    exit 1
fi

# Upload the extracted file to the specified S3 bucket using AWS CLI
aws s3 cp online_retail_II.xlsx s3://online-retail-transaction/input_folder/online_retail_II.xlsx

# Check if the S3 upload was successful
if [ $? -eq 0 ]; then
    echo "Upload to S3 successful"
else
    echo "Upload to S3 failed"
    exit 1
fi
