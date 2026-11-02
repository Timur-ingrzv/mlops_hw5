import os
from confluent_kafka import Producer
import uuid
import random
import time
import json
import logging 
from datetime import datetime
from pathlib import Path
import pandas as pd

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
    "merchant_lon"
]

KAFKA_CONFIG = {
    "transactions_topic": os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions"),
    "bootstrap_servers": os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
}

SEND_DELAY = float(os.getenv("SEND_DELAY", 0.25))
TIME_FORMAT = "%Y-%m-%d %H:%M"
LABELED_DATA_PATH = Path(os.getenv("LABELED_DATA_PATH", "/data/train.csv"))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class Simulator:
    def __init__(self):
        self.producer = Producer({"bootstrap.servers": KAFKA_CONFIG['bootstrap_servers']})

    def prepare_message(self, df_row):
        row = df_row.copy()
        row['transaction_id'] = str(uuid.uuid4())
        row['transaction_time'] = datetime.now().strftime(TIME_FORMAT)
        if 'target' in row.index:
            row.drop('target')
        message = {
            "transaction_id": row['transaction_id'],
            "data": [row.drop('transaction_id').to_dict()]
        }
        return message
    
    def send_to_kafka(self, row, topic):
        try:
            data = self.prepare_message(row)
            self.producer.produce(topic, value=json.dumps(data).encode("utf-8"))
            time.sleep(0.1)
            self.producer.flush()
            return True
        except Exception as e:
            logger.error(e)

    def simulation(self, df, time_delay):
        sent = 0
        while True:
            rand_idx = random.randrange(len(df))
            row = df.iloc[rand_idx]
            self.send_to_kafka(row, KAFKA_CONFIG['transactions_topic'])
            sent += 1
            if sent % 100 == 0:
                logger.info(
                    "%s messages were sent in kafka", 
                    sent
                )
            time.sleep(time_delay)

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
    
    def close(self):
        self.producer.flush()

if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    service = Simulator()
    try:
        df = service.load_csv(LABELED_DATA_PATH)
        service.simulation(df, SEND_DELAY)
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
    finally:
        service.close()
    