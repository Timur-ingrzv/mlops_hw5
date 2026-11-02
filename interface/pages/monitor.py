import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import os

QUERIES_CONFIG = {
    "num_fraud_transactions": os.getenv("NUM_FRAUD_TRANSACTIONS", 10),
    "num_last_transactions": os.getenv("NUM_LAST_TRANSACTIONS", 100)
}

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "fraud_detections"),
    "table": os.getenv("POSTGRES_TABLE", "scores"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "123456"),
    "host": os.getenv("POSTGRES_HOST", "database"),
    "port": os.getenv("POSTGRES_PORT", "5432")
}

RESULT_COLUMNS = ["transaction_id", "score", "fraud_flag"]

class DBReader:
    def __init__(self):
        connection_config = {k:v for k, v in DB_CONFIG.items() if k != 'table'}
        self.conn = psycopg2.connect(**connection_config)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def _execute(self, query):
        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=RESULT_COLUMNS)
    
    def get_fraud_transactions(self, n):
        try:
            query = f"""
                SELECT transaction_id, score, fraud_flag
                FROM {DB_CONFIG['table']}
                WHERE fraud_flag = 1
                ORDER BY created_at DESC
                LIMIT {n};
            """
            return self._execute(query)
        
        except Exception as e:
            st.error(f'Error while collecting stats: {str(e)}')
            return pd.DataFrame([], columns=RESULT_COLUMNS)

    def get_last_transactions(self, n):
            try:
                query = f"""
                    SELECT transaction_id, score, fraud_flag
                    FROM {DB_CONFIG['table']}
                    ORDER BY created_at DESC
                    LIMIT {n};
                """
                return self._execute(query)
                
            except Exception as e:
                st.error(f'Error while collecting stats: {str(e)}')
                return pd.DataFrame([], columns=RESULT_COLUMNS)

    def close(self):
        self.conn.close()

@st.cache_resource
def get_reader() -> DBReader:
    return DBReader()

st.title("Statistics")
if st.button('Show results'):
    reader = get_reader()

    st.divider()

    st.subheader("Fraud transactions")

    fraud_transactions = reader.get_fraud_transactions(QUERIES_CONFIG["num_fraud_transactions"])
    st.dataframe(
        fraud_transactions,
        width='stretch',
        hide_index=True,
    )

    st.divider()

    st.subheader("Scores distribution")

    last_transactions = reader.get_last_transactions(QUERIES_CONFIG['num_last_transactions'])
    fig = px.histogram(
        last_transactions,
        x="score",
        nbins=20,
        title="Fraud score distribution"
    )
    st.plotly_chart(
        fig,
        width='stretch'
    )
