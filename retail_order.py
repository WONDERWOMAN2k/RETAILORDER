# -*- coding: utf-8 -*-
"""Retail_Order.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1gJ7YS17oIEMAyIph5dITws_BFh5OnMBB
"""

import subprocess
import sys

# Function to install packages
def install_packages():
    packages = ["kaggle", "pymysql", "streamlit"]
    for package in packages:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

install_packages()


from google.colab import files
files.upload()

import os

# Create the .kaggle directory if it doesn't exist
kaggle_dir = os.path.expanduser("~/.kaggle")
os.makedirs(kaggle_dir, exist_ok=True)


import os

# Rename to .zip if needed
os.rename("orders.csv", "orders.zip")

# Extract the ZIP file
!unzip -o orders.zip -d extracted_data

# Check extracted files
!ls extracted_data

!rm -f orders.csv orders.zip
!kaggle datasets download -d ankitbansal06/retail-orders --force
!unzip -o retail-orders.zip -d extracted_data

import pandas as pd

# Load the dataset
df = pd.read_csv("extracted_data/orders.csv", encoding="ISO-8859-1")

# Display the first few rows
df.head()

df.info()  # Check column types
df.isnull().sum()  # Check missing values
df.describe()  # Get basic statistics

!mv extracted_data/orders.csv orders.csv
df = pd.read_csv("orders.csv", encoding="ISO-8859-1")
!find /content -name "orders.csv"

import pandas as pd

df = pd.read_csv("orders.csv")
df.head()

df.fillna(0, inplace=True)  # Replace missing values with 0

df.columns = df.columns.str.lower().str.replace(' ', '_')  # Convert to lowercase & remove spaces
df.rename(columns={'order id': 'order_id', 'sale price': 'sale_price'}, inplace=True)

print(df.columns)


df.columns = df.columns.str.strip()  # Remove spaces
print(df.columns)  # Check updated column names
df.rename(columns={"Sale Price": "sale_price"}, inplace=True)

print(df.columns)
df.columns = df.columns.str.strip()  # Remove spaces
print(df.columns)  # Check updated column names
print(df["cost_price"].head())  # Check if the issue is fixed

df["sale_price"] = df["list_price"] * (1 - df["discount_percent"] / 100)

# Check if it worked
print(df[["list_price", "discount_percent", "sale_price"]].head())

print(df.info())  # Check if sale_price exists
print(df.head())  # Preview data

!pip install pymysql
!pip install mysql-connector-python


import mysql.connector
from mysql.connector import Error

!pip install mysql-connector-python
!wget https://download.pingcap.org/tidb-cloud-ca.pem -O /content/ca.pem
!ls /content

config = {
    "host": "gateway01.us-west-2.prod.aws.tidbcloud.com",
    "port": 4000,
    "user": "2cG3MBTK8AjfDHM.root",
    "password": "your_password",
    "database": "retailorder",
    "ssl_disabled": True  # Temporarily disable SSL
}

from google.colab import files
uploaded = files.upload()

from google.colab import drive
drive.mount('/content/drive')

!ls "/content/drive/My Drive/Colab Notebooks/"

import os
print(os.path.exists("isrgrootx1.pem"))

from google.colab import drive
import pandas as pd

# Mount Google Drive
drive.mount('/content/drive')

# Sample DataFrame
df = pd.DataFrame({"OrderID": [1, 2, 3], "Product": ["A", "B", "C"]})

# Save to Google Drive
csv_path = "/content/drive/My Drive/Orders.csv"
df.to_csv(csv_path, index=False)

print(f"File saved to {csv_path}")

import pandas as pd

# Load CSV file (Make sure the path is correct)
df = pd.read_csv("/content/drive/My Drive/Orders.csv")

# Handle NaN values to avoid SQL errors
df = df.where(pd.notna(df), None)
df.rename(columns={"Order Date": "order_date", "Date Ordered": "order_date"}, inplace=True)
df = pd.read_csv("/content/drive/My Drive/Orders.csv", sep=";")
print(df.head())

import pandas as pd

# Load CSV with default settings
df = pd.read_csv("/content/drive/My Drive/Orders.csv")

# Remove spaces from column names
df.columns = df.columns.str.strip()

# Print available columns
print("Columns in DataFrame:", df.columns)

# Rename columns if needed
if "Order Date" in df.columns:
    df.rename(columns={"Order Date": "order_date"}, inplace=True)

# Verify 'order_date' exists
if "order_date" in df.columns:
    # Convert to correct date format
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    df['order_date'] = df['order_date'].dt.strftime('%Y-%m-%d')
else:
    print("❌ 'order_date' column not found! Trying different CSV separators...")

    # Try different separators
    df = pd.read_csv("/content/drive/My Drive/Orders.csv", sep=",")
    print("After using ',' as separator:\n", df.head())

    df = pd.read_csv("/content/drive/My Drive/Orders.csv", sep=";")
    print("After using ';' as separator:\n", df.head())

    df = pd.read_csv("/content/drive/My Drive/Orders.csv", engine="python")
    print("After using engine='python':\n", df.head())

# Print final DataFrame
print("\n✅ Final DataFrame preview:")
print(df.head())

!ls /content/drive/My\ Drive/

import os

# Function to search for a file by name starting from a given directory
def find_file(start_dir, target_filename):
    for root, dirs, files in os.walk(start_dir):
        if target_filename in files:
            return os.path.join(root, target_filename)
    return None

# Example: Searching for "isrgrootx1.pem" in Google Drive after mounting
file_path = find_file("/content/drive", "isrgrootx1.pem")
print("File found at:", file_path)

!find "/content/drive/My Drive/" -name "isrgrootx1.pem"

import mysql.connector

# Correct configuration with the certificate path from Google Drive
config = {
    "host": "gateway01.us-west-2.prod.aws.tidbcloud.com",
    "port": 4000,
    "user": "2cG3MBTK8AjfDHM.root",  # Your username
    "password": "dYaKCArJUfrmgU85",  # Your password
    "database": "retailorder",  # Your database
    "ssl_ca": "/content/drive/My Drive/Colab Notebooks/isrgrootx1.pem",  # Correct SSL certificate path
    "ssl_verify_cert": True,  # Enforce certificate verification
    "ssl_disabled": False  # Ensure secure connection
}

# Establish connection
connection = mysql.connector.connect(**config)
cursor = connection.cursor()

# Test connection

import mysql.connector

# Reconnect to TiDB Cloud
connection = mysql.connector.connect(**config)
cursor = connection.cursor()

create_table_query = """
CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY,
    order_date DATE,
    ship_mode VARCHAR(255),
    segment VARCHAR(255),
    country VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    postal_code VARCHAR(20),
    region VARCHAR(255),
    category VARCHAR(255),
    sub_category VARCHAR(255),
    product_id VARCHAR(255),
    cost_price FLOAT,
    list_price FLOAT,
    quantity INT,
    discount_percent FLOAT
);
"""

cursor.execute(create_table_query)
connection.commit()
print("✅ Orders table created successfully!")
cursor.close()
connection.close()

connection = mysql.connector.connect(**config)
cursor = connection.cursor()

cursor.execute("SELECT COUNT(*) FROM orders;")
result = cursor.fetchone()
print("Total Orders:", result[0])

cursor.close()
connection.close()
