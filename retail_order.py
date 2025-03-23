import os
import shutil
import pandas as pd
import streamlit as st
import mysql.connector

# Streamlit file uploader to upload kaggle.json file
uploaded_file = st.file_uploader("Upload kaggle.json", type=["json"])

if uploaded_file is not None:
    # Create the .kaggle directory if it doesn't exist
    os.makedirs(os.path.expanduser("~/.kaggle"), exist_ok=True)

    # Save the uploaded file to the appropriate location
    with open(os.path.expanduser("~/.kaggle/kaggle.json"), "wb") as f:
        f.write(uploaded_file.getbuffer())
    st.success("kaggle.json uploaded successfully!")

# TiDB Cloud configuration
config = {
    "host": "gateway01.us-west-2.prod.aws.tidbcloud.com",
    "port": 4000,
    "user": "2cG3MBTK8AjfDHM.root",  # Your username
    "password": "dYaKCArJUfrmgU85",  # Your password
    "database": "retailorder",  # Database name
    "ssl_ca": "/path/to/isrgrootx1.pem",  # Path to the SSL certificate (adjust for your environment)
    "ssl_verify_cert": True,  # Enforce certificate verification
    "ssl_disabled": False  # Ensure secure connection
}

# Example to load and clean your data
df = pd.read_csv("orders.csv", encoding="ISO-8859-1")
df.fillna(0, inplace=True)
df.columns = df.columns.str.lower().str.replace(' ', '_')
df.rename(columns={'order id': 'order_id', 'sale price': 'sale_price'}, inplace=True)
df["sale_price"] = df["list_price"] * (1 - df["discount_percent"] / 100)

# Print dataset columns to verify correct mapping
print("Dataset Columns:", df.columns)

# Database interaction
connection = None
cursor = None

try:
    connection = mysql.connector.connect(**config)  # Create the connection
    cursor = connection.cursor()  # Initialize the cursor

    # Create table in TiDB Cloud
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
    cursor.execute(create_table_query)  # Execute table creation query
    connection.commit()  # Commit the transaction
    print("✅ Orders table created successfully!")

    # Insert data into the database
    insert_query = """
    INSERT INTO orders (order_id, order_date, ship_mode, segment, country, city, state, postal_code, region, category, sub_category, product_id, cost_price, list_price, quantity, discount_percent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for index, row in df.iterrows():
        values = (
            row['order_id'], row['order_date'], row['ship_mode'], row['segment'], row['country'],
            row['city'], row['state'], row['postal_code'], row['region'], row['category'],
            row['sub_category'], row['product_id'], row['cost_price'], row['list_price'],
            row['quantity'], row['discount_percent']
        )
        cursor.execute(insert_query, values)  # Insert row into the database
    connection.commit()  # Commit all inserts

    # Check the total number of orders
    cursor.execute("SELECT COUNT(*) FROM orders;")
    result = cursor.fetchone()
    print("Total Orders:", result[0])

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    if connection and connection.is_connected():
        cursor.close()  # Close the cursor
        connection.close()  # Close the database connection
        print("Connection closed.")

# Checking cleaned data
st.write(df.head())

# Handle missing order_id
st.write("Rows with missing or invalid order_id:")
invalid_order_ids = df[df['order_id'].isnull()]
st.write(invalid_order_ids)

# Clean the data
df.dropna(subset=['order_id'], inplace=True)
df['order_id'] = pd.to_numeric(df['order_id'], errors='coerce')
df.dropna(subset=['order_id'], inplace=True)
st.write("Cleaned data:")
st.write(df.head())

