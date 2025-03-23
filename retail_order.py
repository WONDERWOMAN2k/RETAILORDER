import subprocess
import sys
import os
import zipfile
import pandas as pd
import mysql.connector
from kaggle.api.kaggle_api_extended import KaggleApi

# Function to install packages
def install_packages():
    packages = ["kaggle", "pymysql", "streamlit"]
    for package in packages:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

install_packages()

# Create the .kaggle directory if it doesn't exist
kaggle_dir = os.path.expanduser("~/.kaggle")
os.makedirs(kaggle_dir, exist_ok=True)

# Rename to .zip if needed
if os.path.exists("orders.csv"):
    os.rename("orders.csv", "orders.zip")

# Extract the zip file
zip_file = 'orders.zip'
extract_dir = 'extracted_data'
os.makedirs(extract_dir, exist_ok=True)

# Extract the zip file
with zipfile.ZipFile(zip_file, 'r') as zip_ref:
    zip_ref.extractall(extract_dir)

# List the files in the extracted_data directory
files = os.listdir('extracted_data')
print(files)

# List of files to remove
files_to_remove = ['orders.csv', 'orders.zip']
for file in files_to_remove:
    if os.path.exists(file):
        os.remove(file)
        print(f"Removed {file}")
    else:
        print(f"{file} not found")

# Authenticate with Kaggle (ensure you have the Kaggle API key stored in ~/.kaggle/kaggle.json)
api = KaggleApi()
api.authenticate()

# Define dataset and download path
dataset = 'ankitbansal06/retail-orders'
download_path = './'

# Download the dataset
api.dataset_download_files(dataset, path=download_path, unzip=True)
print(f"Dataset {dataset} downloaded and extracted to {download_path}")

# Load the dataset
df = pd.read_csv("extracted_data/orders.csv", encoding="ISO-8859-1")
df.head()

# Data information and cleaning
df.info()  # Check column types
df.isnull().sum()  # Check missing values
df.describe()  # Get basic statistics

# Handle missing values
df.fillna(0, inplace=True)  # Replace missing values with 0

# Clean column names
df.columns = df.columns.str.lower().str.replace(' ', '_')  # Convert to lowercase & remove spaces
df.rename(columns={'order id': 'order_id', 'sale price': 'sale_price'}, inplace=True)

# Calculate sale price
df["sale_price"] = df["list_price"] * (1 - df["discount_percent"] / 100)

# Check if it worked
print(df[["list_price", "discount_percent", "sale_price"]].head())

# Install mysql-connector-python
subprocess.check_call([sys.executable, "-m", "pip", "install", "mysql-connector-python"])

# TiDB Cloud setup
config = {
    "host": "gateway01.us-west-2.prod.aws.tidbcloud.com",
    "port": 4000,
    "user": "2cG3MBTK8AjfDHM.root",
    "password": "your_password",  # Replace with actual password
    "database": "retailorder",
    "ssl_disabled": True  # Temporarily disable SSL
}

# Connect to TiDB Cloud and create table
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

# Insert data into the database (example)
insert_query = """
INSERT INTO orders (order_id, order_date, ship_mode, segment, country, city, state, postal_code, region, category, sub_category, product_id, cost_price, list_price, quantity, discount_percent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
for index, row in df.iterrows():
    cursor.execute(insert_query, tuple(row))
connection.commit()

# Check total number of orders
cursor.execute("SELECT COUNT(*) FROM orders;")
result = cursor.fetchone()
print("Total Orders:", result[0])

cursor.close()
connection.close()
