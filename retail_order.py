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

# ✅ Upload SSL Certificate
ssl_cert = st.file_uploader("Upload SSL Certificate (.pem)", type=["pem"])
ssl_cert_path = None
