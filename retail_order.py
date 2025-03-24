import streamlit as st
import os
import pandas as pd
import mysql.connector

# Title
st.title("Retail Order Analysis")

# Upload Kaggle API key
uploaded_file = st.file_uploader("Upload kaggle.json", type=["json"])
if uploaded_file is not None:
    os.makedirs(os.path.expanduser("~/.kaggle"), exist_ok=True)
    with open(os.path.expanduser("~/.kaggle/kaggle.json"), "wb") as f:
        f.write(uploaded_file.getbuffer())
    st.success("✅ kaggle.json uploaded successfully!")

# Set SSL path correctly
os.environ["SSL_CERT_PATH"] = "/content/isrgrootx1.pem"

# Upload CSV file
uploaded_csv = st.file_uploader("Upload Orders CSV", type=["csv"])
if uploaded_csv is not None:
    df = pd.read_csv(uploaded_csv, encoding="ISO-8859-1")
    st.success("✅ File uploaded successfully!")
    
    # Data Cleaning
    df.fillna(0, inplace=True)
    df.columns = df.columns.str.lower().str.replace(' ', '_')  # Standardize column names
    df["sale_price"] = df["list_price"] * (1 - df["discount_percent"] / 100)

    st.write("🔍 **Cleaned Data Preview:**")
    st.write(df.head())

    # Database Configuration
    config = {
        "host": "gateway01.us-west-2.prod.aws.tidbcloud.com",
        "port": 4000,
        "user": "2cG3MBTK8AjfDHM.root",
        "password": "dYaKCArJUfrmgU85",
        "database": "retailorder",
        "ssl_ca": "/content/isrgrootx1.pem",
        "ssl_verify_cert": True,
        "ssl_disabled": False
    }

    # Database connection and operations
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        # Create Orders table
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
            list_price FLOAT,
            quantity INT,
            discount_percent FLOAT,
            sale_price FLOAT
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
        st.success("✅ Orders table created successfully!")

        # Insert Data into Database
        insert_query = """
        INSERT INTO orders (order_id, order_date, ship_mode, segment, country, city, state, postal_code, region, category, sub_category, product_id, list_price, quantity, discount_percent, sale_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            values = (
                row['order_id'], row['order_date'], row['ship_mode'], row['segment'], row['country'],
                row['city'], row['state'], row['postal_code'], row['region'], row['category'],
                row['sub_category'], row['product_id'], row['list_price'], row['quantity'],
                row['discount_percent'], row['sale_price']
            )
            cursor.execute(insert_query, values)
        connection.commit()
        st.success("✅ Data inserted successfully!")

        # Fetch total number of orders
        cursor.execute("SELECT COUNT(*) FROM orders;")
        result = cursor.fetchone()
        st.write(f"📊 **Total Orders in Database:** {result[0]}")

    except mysql.connector.Error as err:
        st.error(f"❌ Database Error: {err}")

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            st.write("🔌 Database connection closed.")

else:
    st.warning("⚠️ Please upload a CSV file to proceed.")
