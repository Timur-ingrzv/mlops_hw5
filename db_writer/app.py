import psycopg2
import os
import logging
import json
from confluent_kafka import Consumer, Producer
from prometheus_client import Counter, Gauge, Histogram, Summary, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

SCORE_BUCKETS = [i/50 for i in range(51)]
PROCESSING_TIME = Summary(
    "scoring_write_processing_time",
    "Время записи результата скоринга в БД"
)
SCORING_WRITE_TOTAL = Counter(
    "scoring_write_total",
    "Количество записанных результатов"
)
SCORING_WRITE_FRAUD = Counter(
    "scoring_write_fraud",
    "Количество записанных мошеннических транзакций"
)
SCORING_FRAUD_HISTOGRAM = Histogram(
    'scoring_write_fraud_score',
    'Распределение записанных скоров мошенничества',
    buckets=SCORE_BUCKETS,
)

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "fraud_detections"),
    "table": os.getenv("POSTGRES_TABLE", "scores"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "123456"),
    "host": os.getenv("POSTGRES_HOST", "database"),
    "port": os.getenv("POSTGRES_PORT", "5432")
}

KAFKA_CONFIG = {
    "scoring_topic": os.getenv("KAFKA_SCORING_TOPIC", "scoring"),
    "bootstrap_servers": os.getenv("BOOTSTRAP_SERVERS", "kafka:9092"),
    "scoring_group": os.getenv("KAFKA_SCORING_GROUP", 'ml-scorer')
}

METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))

class EventsWriter:
    def __init__(self):
        connection_config = {k:v for k, v in DB_CONFIG.items() if k != 'table'}
        self.conn = psycopg2.connect(**connection_config)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'],
            'group.id': KAFKA_CONFIG['scoring_group'],
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([KAFKA_CONFIG['scoring_topic']])
        start_http_server(METRICS_PORT)

    @PROCESSING_TIME.time()
    def _process_message(self, message):
        try:
            data = json.loads(message.value().decode('utf-8'))[0]
            query = f"""
                INSERT INTO {DB_CONFIG['table']} 
                (transaction_id, us_state, merch, cat_id, amount, score, fraud_flag, model_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO UPDATE SET
                score = EXCLUDED.score,
                fraud_flag = EXCLUDED.fraud_flag
            """
            self.cursor.execute(query, (
                data["transaction_id"],
                data["us_state"],
                data["merch"],
                data["cat_id"],
                data["amount"],
                float(data["score"]),
                int(data["fraud_flag"]),
                data["model_version"]
            ))
    
            score = float(data['score'])
            fraud_flag = int(data['fraud_flag'])
            SCORING_WRITE_TOTAL.inc()
            SCORING_FRAUD_HISTOGRAM.observe(score)
            if fraud_flag == 1:
                SCORING_WRITE_FRAUD.inc()
        
        except json.JSONDecodeError as je:
            logger.exception(f"JSON decoding error: {je}")
    
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def process_messages(self):
        while True:
            message = self.consumer.poll(1.0)
            if not message:
                continue
            if message.error():
                logger.error(f"Kafka error: {message.error()}")
                continue
            self._process_message(message)
            
    def close(self):
        self.cursor.close()
        self.conn.close()
        self.consumer.close()

if __name__ == "__main__":
    logger.info('Starting writer service...')
    writer = EventsWriter()
    try:
        writer.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
    finally:
        writer.close()

