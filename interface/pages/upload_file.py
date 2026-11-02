import streamlit as st
import pandas as pd
from confluent_kafka import Producer
import os
import json
import uuid
import time

KAFKA_CONFIG = {
    "transactions_topic": os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions"),
    "bootstrap_servers": os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
}

def load_file(uploaded_file):
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_server):
    try:
        producer = Producer({"bootstrap.servers": bootstrap_server})
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]

        progress_bar = st.progress(0)
        total_rows = df.shape[0]
        for i, df in df.iterrows():
            data = {
                "transaction_id": df['transaction_id'],
                "data": [df.drop('transaction_id').to_dict()]
            }
            producer.produce(topic, value=json.dumps(data).encode("utf-8"))
            progress_bar.progress((i + 1) / total_rows)
            time.sleep(0.1)
        producer.flush()
        return True
    
    except Exception as e:
        st.error(f'Error while sending file: {str(e)}')
        return False


if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

st.title("Score transactions for fraud identification")
st.divider()
uploaded_file = st.file_uploader(
    "Upload your file with transactions",
    type=["csv"]
)

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
    st.session_state.uploaded_files[uploaded_file.name] = {
        "status": "Uploaded",
        "df": load_file(uploaded_file)
    }
    st.success("File successfully loaded!", icon="✅")

if len(st.session_state.uploaded_files) > 0:
    for file_name, file_data in st.session_state.uploaded_files.items():
        cols = st.columns([2, 2, 4])
        with cols[0]:
            st.markdown(f"**File:** '{file_name}'")
        with cols[1]:
            st.markdown(f"**Status:** `{file_data['status']}`")
        with cols[2]:
            if st.button('Send file', key=f"send_{file_name}"):
                if file_data['df'] is not None:
                    with st.spinner("Sending..."):
                        success = send_to_kafka(
                            file_data['df'],
                            KAFKA_CONFIG['transactions_topic'],
                            KAFKA_CONFIG['bootstrap_servers']
                        )
                        if success:
                            st.session_state.uploaded_files[file_name]['status'] = "Sent"
                            st.rerun()
                else:
                    st.error('No data')
