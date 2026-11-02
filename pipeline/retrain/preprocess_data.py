import logging
import sys
import joblib
from datetime import datetime, timezone
import pandas as pd

from pipeline.settings import FRAUD_SRC, SPLITS_DIR, MODELS_DIR
from pipeline.db import COLUMNS

logger = logging.getLogger(__name__)


def preprocess_dataset(split_info: dict) -> dict:
    """
    Проводим валидацию колонок и их типов датасета перед препроцессингом.
    Проводим препроцессинг с помощью fraud_detector/src/data_preprocessing.
    Возвращает пути к ним и метаданные месяца, нужные следующим задачам.
    """
    if not FRAUD_SRC.exists():
        raise FileNotFoundError(f'Preprocessor not found: {FRAUD_SRC}')
    train = pd.read_parquet(split_info['train_raw_path'])
    test = pd.read_parquet(split_info['test_raw_path'])
    if set(train.columns) != set(COLUMNS):
        raise RuntimeError('Set of train dataset columns are not expected')
    if set(test.columns) != set(COLUMNS):
        raise RuntimeError('Set of test dataset columns are not expected')

    fraud_src_str = str(FRAUD_SRC)
    if fraud_src_str not in sys.path:
        sys.path.insert(0, fraud_src_str)
    from data_preprocessing import preprocess_data

    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    staging_encoder = MODELS_DIR / f".staging_target_encoder_{version}.pkl"
    train = preprocess_data(train, encoder_path=staging_encoder, mode='retrain_train') 
    test = preprocess_data(test, encoder_path=staging_encoder, mode='retrain_inference')
    train_path = SPLITS_DIR / "train_proc.parquet"
    test_path = SPLITS_DIR / "test_proc.parquet"
    train.to_parquet(train_path, index=False)
    test.to_parquet(test_path, index=False)
    logger.info(
        "After preprocessing: train %s, test %s",
        train.shape,
        test.shape
    )
    return {
        "train_proc_path": str(train_path),
        "test_proc_path": str(test_path),
        "batch_month": split_info['batch_month'],
        "train_months": split_info['train_months'],
        "test_month": split_info['test_month'],
        "dataset_path": split_info["dataset_path"],
        "version": version
    }

