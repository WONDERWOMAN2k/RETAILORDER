import streamlit as st
import os
import pandas as pd
import mysql.connector

# Title
st.title("📊 Retail Order Analysis")

# ✅ Upload Kaggle API key (kaggle.json)
uploaded_kaggle = st.file_uploader("Upload kaggle.json", type=["json"])
if uploaded_kaggle is not None:
    kaggle_path = os.path.expanduser("~/.kaggle")
    os.makedirs(kaggle_path, exist_ok=True)
    kaggle_file_path = os.path.join(kaggle_path, "kaggle.json")

    with open(kaggle_file_path, "wb") as f:
        f.write(uploaded_kaggle.getbuffer())

    st.success(f"✅ kaggle.json uploaded successfully!")

# ✅ Upload SSL Certificate (.pem)
uploaded_ssl = st.file_uploader("Upload SSL Certificate (.pem)", type=["pem"])
ssl_cert_path = None
if uploaded_ssl is not None:
    ssl_cert_path = os.path.join(os.getcwd(), "ssl_certificate.pem")

    with open(ssl_cert_path, "wb") as f:
        f.write(uploaded_ssl.getbuffer())

    st.success(f"✅ SSL Certificate uploaded successfully!")

# ✅ Upload Orders CSV file
uploaded_csv = st.file_uploader("Upload Orders CSV", type=["csv"])
if uploaded_csv is not None:
    try:
        df = pd.read_csv(uploaded_csv, encoding="ISO-8859-1")
    except UnicodeDecodeError:
        df = pd.read_csv(uploaded_csv, encoding="utf-8")

    st.success("✅ Orders CSV uploaded successfully!")

    # ✅ Display CSV Columns for Debugging
    st.write("🔍 **CSV Columns Detected:**", df.columns.tolist())

    # ✅ Data Cleaning
    df.fillna(0, inplace=True)
    df.columns = df.columns.str.lower().str.replace(' ', '_')  # Standardize column names
    df["sale_price"] = df["list_price"] * (1 - df["discount_percent"] / 100)

    # ✅ Convert data types
    try:
        df["order_id"] = df["order_id"].astype(int)
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date  # Convert to date
        df["list_price"] = df["list_price"].astype(float)
        df["quantity"] = df["quantity"].astype(int)
        df["discount_percent"] = df["discount_percent"].astype(float)
        df["sale_price"] = df["sale_price"].astype(float)
    except Exception as e:
        st.error(f"❌ Data Type Conversion Error: {e}")

    # ✅ Display data preview
    st.write("🔍 **Cleaned Data Preview:**")
    st.write(df.head())

    # ✅ Database Configuration (TiDB Cloud)
    config = {
        "host": "gateway01.us-west-2.prod.aws.tidbcloud.com",
        "port": 4000,
        "user": "2cG3MBTK8AjfDHM.root",
        "password": "dYaKCArJUfrmgU85",
        "database": "retailorder",
    }
    if ssl_cert_path:  # Use SSL if uploaded
        config["ssl_ca"] = ssl_cert_path
        config["ssl_verify_cert"] = True

    # ✅ Database connection and operations
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        # ✅ Create Orders table if not exists
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

        # ✅ Check if required columns exist
        required_columns = [
            'order_id', 'order_date', 'ship_mode', 'segment', 'country', 'city', 'state',
            'postal_code', 'region', 'category', 'sub_category', 'product_id',
            'list_price', 'quantity', 'discount_percent', 'sale_price'
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            st.error(f"❌ Missing columns in CSV: {missing_columns}")
        else:
            # ✅ Prepare data for batch insertion
            data_tuples = [
                (
                    int(row["order_id"]), row["order_date"], row["ship_mode"], row["segment"],
                    row["country"], row["city"], row["state"], row["postal_code"], row["region"],
                    row["category"], row["sub_category"], row["product_id"], float(row["list_price"]),
                    int(row["quantity"]), float(row["discount_percent"]), float(row["sale_price"])
                )
                for _, row in df.iterrows()
            ]

            # ✅ Display sample row before insertion
            if data_tuples:
                st.write("🔍 **Sample row to insert:**", data_tuples[0])

                # ✅ Insert Data into Database (Optimized)
                insert_query = """
                INSERT IGNORE INTO orders (order_id, order_date, ship_mode, segment, country, city, state, postal_code, region, category, sub_category, product_id, list_price, quantity, discount_percent, sale_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                cursor.executemany(insert_query, data_tuples)  # ✅ Faster batch insert
                connection.commit()
                st.success(f"✅ Successfully inserted {len(data_tuples)} records!")

                # ✅ Fetch total number of orders
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
