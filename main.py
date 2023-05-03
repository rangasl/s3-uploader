import boto3
import pandas as pd
import os
import datetime
import configparser
import pyodbc
import mysql.connector

# Read credentials from config file
config = configparser.ConfigParser()
config.read('config.ini')
db_type = config.get('database', 'type')
host = config.get('database', 'host')
database = config.get('database', 'database')
user = config.get('database', 'user')
password = config.get('database', 'password')
access_key = config.get('s3', 'access_key')
secret_key = config.get('s3', 'secret_key')
bucket_name = config.get('s3', 'bucket_name')
folder_name = config.get('s3', 'folder_name')

# Connect to database
if db_type == 'mysql':
    cnxn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    query = "SELECT * FROM users"
elif db_type == 'sqlserver':
    cnxn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};DATABASE={database};UID={user};PWD={password}"
    )
    query = "SELECT * FROM dbo.users"

# Execute database query and create pandas dataframe
df = pd.read_sql(query, cnxn)

# Get today's date for file naming
today = datetime.datetime.now().strftime("%Y-%m-%d")

# Write dataframe to CSV file
file_name = f"{today}_data.csv"
df.to_csv(file_name, index=False)

# Rename file with yesterday's date
yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
try:
    os.rename(file_name, f"{yesterday}_data.csv")
except Exception as e:
    print("Error renaming file:", e)

# Authenticate with AWS using boto3
session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

# Upload file to S3 bucket
s3 = session.resource('s3')
s3_file_name = f"{folder_name}/{yesterday}_data.csv"

try:
    s3.Bucket(bucket_name).upload_file(f"{yesterday}_data.csv", s3_file_name)
    print("File uploaded to S3 successfully")
except Exception as e:
    print("Error uploading file to S3:", e)

# Close database connection
cnxn.close()
