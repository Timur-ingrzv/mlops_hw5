from pathlib import Path
import os

FRAUD_SRC = Path(os.getenv("FRAUD_SRC", "/opt/airflow/fraud_src"))
MODELS_DIR = Path(os.getenv("MODELS_DIR", "/opt/airflow/models"))
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "/opt/airflow/reports"))
ENCODER_PATH = MODELS_DIR / "target_encoder.pkl"

TRAINING_DIR = Path(os.getenv('TRAINING_DATA_DIR', "/data/training_data"))
SUPPLY_BATCH = TRAINING_DIR / "supply_batch.parquet"
CURRENT_DATASET = TRAINING_DIR / "current.parquet"
PENDING_DATASET = TRAINING_DIR / "pending.parquet"
SPLITS_DIR = TRAINING_DIR / "splits"

SAMPLE_SIZE = 50_000
HISTORY_YEARS = 3
F1_EPS = 0.5

DQ_RULES = {
    "target_balance_min": 0.005, 
    "missing_ratio_max": 0.05,
    "amount_min": 0.0,
    "amount_zscore_max": 10.0,
    "amount_outlier_ratio_max": 0.01,
    "duplicate_ratio_max": 0.001,
    "feature_std_min": 1e-6,
}
