import os
import sys
import pandas as pd
import time
import logging
import threading 
import json
import joblib
from pathlib import Path
from confluent_kafka import Consumer, Producer
from prometheus_client import Counter, Gauge, Histogram, Summary, start_http_server
from sklearn.preprocessing import TargetEncoder

sys.path.append(os.path.abspath('./src'))
from data_preprocessing import preprocess_data
from scoring import get_preds

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

PROCESSING_TIME = Summary(
    'transaction_processing_seconds',
    'Время обработки одной транзакции'
)
TRANSACTION_COUNT = Counter(
    'transactions_total',
    'Общее количество обработанных транзакций'
)
FRAUD_SCORE = Histogram(
    'scores_fraud',
    'Распределение скоров мошеннических транзакций',
    buckets=[i / 50.0 for i in range(51)]
)
FRAUD_RATIO = Gauge(
    'scores_fraud_rate',
    'Доля мошеннических транзакций'
)
MODEL_VERSION_UPDATES = Counter(
    'model_version_updates_total',
    'Смена версии модели',
    ['version'],
)
UNPROCESSED_MESSAGES = Gauge(
    'transactions_unprocessed',
    'Количество необработанных сообщений в топике транзакций по партициям',
    ['topic', 'partition']
)
UNPROCESSED_MESSAGES_TOTAL = Gauge(
    'transactions_unprocessed_total',
    'Общее количество необработанных сообщений в топике транзакций',
    ['topic']
)

KAFKA_CONFIG = {
    "transactions_topic": os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions"),
    "scoring_topic": os.getenv("KAFKA_SCORING_TOPIC", "scoring"),
    "bootstrap_servers": os.getenv("BOOTSTRAP_SERVERS", "kafka:9092"),
    "transactions_group": os.getenv("KAFKA_TRANSACTIONS_GROUP", 'ml-scorer')
}

MODELS_DIR = Path(os.getenv("MODELS_DIR", "/models"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

class ModelUpdater:
    def __init__(self):
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        latest = MODELS_DIR / "latest.txt"
        if not latest.exists() or not latest.read_text(encoding="utf-8").strip():
            dest = self._model_path("initial")
            if not dest.exists():
                raise FileNotFoundError(
                    "Initial model doesn't exist"
                )
            latest.write_text("initial\n", encoding="utf-8")

        self.current_version = self._read_latest_version()
        self.model = self._load_model(self.current_version)

        start_http_server(METRICS_PORT)
        
    def _model_path(self, version: str) -> Path:
        return MODELS_DIR / f"my_classifier_{version}.pkl"

    def _read_latest_version(self) -> str:
        version = (MODELS_DIR / 'latest.txt').read_text(encoding='utf-8').strip()
        if not version:
            raise RuntimeError('No model pointer in latest.txt')
        return version

    def _load_model(self, version):
        path = self._model_path(version)
        model = joblib.load(path)
        return model

    def _current_model(self):
        version = self._read_latest_version()
        if version != self.current_version:
            self.model = self._load_model(version)
            self.current_version = version
        return self.model, self.current_version

class EncoderUpdater:
    def __init__(self):
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        latest = MODELS_DIR / "latest.txt"
        if not latest.exists() or not latest.read_text(encoding="utf-8").strip():
            dest = self._model_path("initial")
            if not dest.exists():
                raise FileNotFoundError(
                    "Initial model doesn't exist"
                )
            latest.write_text("initial\n", encoding="utf-8")

        self.current_version = self._read_latest_version()
        self.model = self._load_model(self.current_version)
        
    def _model_path(self, version: str) -> Path:
        return MODELS_DIR / f"target_encoder_{version}.pkl"

    def _read_latest_version(self) -> str:
        version = (MODELS_DIR / 'latest.txt').read_text(encoding='utf-8').strip()
        if not version:
            raise RuntimeError('No encoder pointer in latest_encoder.txt')
        return version

    def _load_model(self, version):
        path = self._model_path(version)
        model = joblib.load(path)
        return model

    def _current_model(self):
        version = self._read_latest_version()
        if version != self.current_version:
            self.model = self._load_model(version)
            self.current_version = version
        return self.model, self.current_version
        

class ProcessingService:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'],
            'group.id': KAFKA_CONFIG['transactions_group'],
            'auto.offset.reset': 'earliest'
        })
        self.producer = Producer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers']
        })
        self.consumer.subscribe([KAFKA_CONFIG['transactions_topic']])

        self.model_updater = ModelUpdater()
        self.encoder_updater = EncoderUpdater()

        self.total_transactions = 0
        self.fraud_transactions = 0
        self._seen_model_version = None

        self.interval = 5.0
        self._lag_stop = threading.Event()
        self._lag_thread = threading.Thread(
            target=self._lag_report,
            name='kafka=lag-report',
            daemon=True
        )
        self._lag_thread.start()

    def _record_model_version(self, model_version):
        if self._seen_model_version is None or self._seen_model_version != model_version:
            MODEL_VERSION_UPDATES.labels(version=model_version).inc()
            logger.info(
                "Модель сменилась: %s -> %s",
                self._seen_model_version,
                model_version
            )
            self._seen_model_version = model_version


    @PROCESSING_TIME.time()
    def _process_message(self, message):
        try:
            data = json.loads(message.value().decode('utf-8'))
            transaction_id = data['transaction_id']
            df = pd.DataFrame(data['data'])
            us_state = df['us_state'].iloc[0]
            merch = df['merch'].iloc[0]
            cat_id = df['cat_id'].iloc[0]
            amount = df['amount'].iloc[0]
        
            df = preprocess_data(df, mode='inference', encoder_updater=self.encoder_updater)
            prediction = get_preds(df, self.model_updater)
            prediction['transaction_id'] = transaction_id
            prediction['us_state'] = us_state
            prediction['merch'] = merch
            prediction['cat_id'] = cat_id
            prediction['amount'] = amount
            logger.info(f"Prediction for transaction {transaction_id} completed")
        
            record = prediction.to_json(orient='records')
            self.producer.produce(
                KAFKA_CONFIG['scoring_topic'],
                value=record.encode('utf-8')
            )
            
            TRANSACTION_COUNT.inc()
            score = float(prediction['score'].item())
            FRAUD_SCORE.observe(score)
            if int(prediction['fraud_flag'].item()) == 1:
                self.fraud_transactions += 1
            self.total_transactions += 1
            FRAUD_RATIO.set(self.fraud_transactions / self.total_transactions)
            self._record_model_version(prediction['model_version'].item())
        
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def process_messages(self):
        while True:
            message = self.consumer.poll(1.0)
            if not message:
                time.sleep(0.2)
                continue
            if message.error():
                logger.error(f"Kafka error: {message.error()}")
                continue
            self._process_message(message)

    def _update_lag(self):
        assignment = self.consumer.assignment()
        if not assignment:
            return 
        
        topic = KAFKA_CONFIG['scoring_topic']
        total_lag = 0
        for tp in assignment:
            try:
                low, high = self.consumer.get_watermark_offsets(
                    tp, timeout=1.0, cached=False
                )

                position = self.consumer.position([tp])
                if position[0].offset < 0:
                    continue
                lag = max(high - position[0].offset, 0)
                UNPROCESSED_MESSAGES.labels(
                    topic=topic, partition=str(tp.partition)).set(lag)
                total_lag += lag

            except Exception as e:
                logger.warning(
                    'Failed to get lag for %s: %s', 
                    str(tp.partition), e
                )
        UNPROCESSED_MESSAGES_TOTAL.labels(topic=topic).set(total_lag)

    def _lag_report(self):
        while not self._lag_stop.is_set():
            try:
                self._update_lag()
            except Exception as e:
                logger.error('Lag report error: %s', e)
            self._lag_stop.wait(self.interval)

    def close(self):
        self.consumer.close()
        self.producer.flush(timeout=10)
        self._lag_stop.set()
        if self._lag_thread.is_alive():
            self._lag_thread.join(timeout=5)

if __name__ == "__main__":
    logger.info('Starting Kafka ML scoring service...')
    service = ProcessingService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info('Service stopped by user')
    finally:
        service.close()

