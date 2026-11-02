import logging
import os
import io
import psycopg2
import pandas as pd
from pathlib import Path

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "fraud_detections"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "123456"),
    "host": os.getenv("POSTGRES_HOST", "database"),
    "port": os.getenv("POSTGRES_PORT", "5432")
}

TRAINING_DIR = Path(os.getenv("TRAINING_DIR", "/data/training_data"))
LABELED_DATA_PATH = Path(os.getenv("LABELED_DATA_PATH", "/data/train.csv"))
CURRENT_DATASET = TRAINING_DIR / "current.parquet"

INITIAL_WINDOW_START = "2019-01-01"
INITIAL_WINDOW_END = "2019-03-01"
INITIAL_NEXT_BATCH = "2019-04-01"

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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DBWriter:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def _execute(self, query, params=None,  mode='modify'):
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            if mode == 'read_one':
                return cur.fetchone()
            elif mode == 'read_all':
                return cur.fetchall()

    def load_csv(self, file_path):
        if not file_path.exists():
            raise FileNotFoundError(f"File doesn't exist: {file_path}")

        data = pd.read_csv(file_path)
        missed_cols = [col for col in COLUMNS if col not in data.columns]
        if len(missed_cols) > 0:
            raise ValueError(f"Columns {missed_cols} is not in csv file")

        data = data[COLUMNS]
        data['transaction_time'] = pd.to_datetime(data['transaction_time'])
        logger.info(
            'CSV file is read: %s lines, period of time %s - %s',
            len(data),
            data['transaction_time'].min(),
            data['transaction_time'].max()
        )
        return data

    def row_count(self):
        query = """
            SELECT COUNT(*)
            FROM labeled_transactions
        """
        res = self._execute(query, mode='read_one')[0]
        return res
    
    def copy_frame(self, data):
        buffer = io.StringIO()
        data.to_csv(
            buffer,
            index=False,
            header=False,
            date_format="%Y-%m-%d %H:%M:%S",
        )
        buffer.seek(0)
        with self.conn.cursor() as cur:
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
        self.conn.commit()
        logger.info("%s lines added to database", len(data))

    def refresh_state(self):
        query = """
        CREATE TABLE IF NOT EXISTS training_state (
            id INT PRIMARY KEY,
            window_start DATE NOT NULL,
            window_end DATE NOT NULL,
            next_batch_month DATE NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT now()
        )
        """
        self._execute(query)

        with self.conn.cursor() as cur:
            check_query = """
            SELECT COUNT(*) FROM training_state WHERE id = 1
            """
            cur.execute(check_query)
            if cur.fetchone()[0]:
                logger.info("Time pointer for train already exists, do not refresh it")
                return 
            
        query = """
            INSERT INTO training_state (id, window_start, window_end, next_batch_month)
            VALUES (1, %s, %s, %s)
        """
        params = (INITIAL_WINDOW_START, INITIAL_WINDOW_END, INITIAL_NEXT_BATCH)
        self._execute(query, params)
        logger.info(
            "Initial train period: %s - %s, first DAG batch: %s",
            INITIAL_WINDOW_START,
            INITIAL_WINDOW_END,
            INITIAL_NEXT_BATCH
        )

    def read_state(self):
        query = """
            SELECT window_start, window_end
            FROM training_state
            WHERE id = 1
        """
        res = self._execute(query, mode='read_one')
        return res

    def materialise_dataset(self):
        TRAINING_DIR.mkdir(parents=True, exist_ok=True)
        os.chmod(TRAINING_DIR, 0o777)
        if CURRENT_DATASET.exists():
            logger.info(f'Current dataset {CURRENT_DATASET} exists')
            return

        window_start, window_end = self.read_state()
        end_exclusive = (window_end + pd.DateOffset(months=1)).to_pydatetime()
        query = """
            SELECT transaction_time, merch, cat_id, amount, name_1, name_2, gender,
                street, one_city, us_state, post_code, lat, lon, population_city,
                jobs, merchant_lat, merchant_lon, target
            FROM labeled_transactions
            WHERE transaction_time >= %s AND transaction_time < %s
            ORDER BY transaction_time
        """
        params = (window_start, end_exclusive)
        res = self._execute(query, params, mode='read_all')

        data = pd.DataFrame(res, columns=COLUMNS)
        data.to_parquet(CURRENT_DATASET, index=False)
        os.chmod(CURRENT_DATASET, 0o666)
        logger.info(
            "Dataset created: %s lines, period %s - %s, file: %s",
            len(data),
            data["transaction_time"].min() if len(data) else None,
            data["transaction_time"].max() if len(data) else None,
            CURRENT_DATASET
        )

    def process_labeled_data(self):
        data = self.load_csv(LABELED_DATA_PATH)
        if self.row_count() > 0:
            logger.info('Data loading skipped, data already exists in db')
        else:
            self.copy_frame(data)
        self.refresh_state()
        self.materialise_dataset()

    def close(self):
        self.conn.close()

logger = logging.getLogger(__name__)
if __name__ == "__main__":
    logger.info('Starting data loader service...')
    writer = DBWriter()
    try:
        writer.process_labeled_data()
    except Exception as e:
        logger.error(e)
    finally:
        writer.close()
        logger.info('Finishing data loader service...')