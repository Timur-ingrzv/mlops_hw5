import psycopg2
from dateutil.relativedelta import relativedelta
import os
import io
import logging


logger = logging.getLogger(__name__)

COLUMNS = [
    "transaction_time",
    "merch",
    "cat_id",
    "amount",
    "name_1",
    "name_2",
    "gender",
    "street",
    "one_city",
    "us_state",
    "post_code",
    "lat",
    "lon",
    "population_city",
    "jobs",
    "merchant_lat",
    "merchant_lon",
    "target",
]

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "fraud_detections"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "123456"),
    "host": os.getenv("POSTGRES_HOST", "database"),
    "port": os.getenv("POSTGRES_PORT", "5432")
}

def get_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

def copy_labeled_frame(conn, data):
    buffer = io.StringIO()
    data.to_csv(
        buffer,
        index=False,
        header=False,
        date_format="%Y-%m-%d %H:%M:%S"
    )
    buffer.seek(0)

    with conn.cursor() as cur:
        cur.copy_expert(
            """
            COPY labeled_transactions (
                transaction_time, merch, cat_id, amount, name_1, name_2, gender,
                street, one_city, us_state, post_code, lat, lon, population_city,
                jobs, merchant_lat, merchant_lon, target
            )
            FROM STDIN WITH (FORMAT CSV)
            """,
            buffer
        )
        conn.commit()

def get_state(conn):
    query = """
    SELECT window_start, window_end, next_batch_month
    FROM training_state
    """
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
    if not row:
        raise RuntimeError('Table training_state is empty')
    result = {
        "window_start": row[0],
        "window_end": row[1],
        "next_batch_month": row[2]
    }  
    return result

def update_training_state(conn, batch_month):
    window_end = batch_month
    window_start = batch_month - relativedelta(months=2)
    next_batch = batch_month + relativedelta(months=1)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE training_state
            SET window_start = %s,
                window_end = %s,
                next_batch_month = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = 1
            """,
            (window_start, window_end, next_batch),
        )
        conn.commit()
        logger.info(
            "Training state updated: window %s — %s, next batch month %s",
            window_start,
            window_end,
            next_batch,
        ) 
