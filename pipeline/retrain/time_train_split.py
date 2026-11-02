import logging

import pandas as pd

from pipeline.settings import SPLITS_DIR

logger = logging.getLogger(__name__)


def time_train_test_split(window_info: dict) -> dict:
    """Делит окно по времени: два более ранних месяца в train, свежий месяц в test.

    Читает parquet окна из window_info, сортирует по transaction_time и
    сохраняет сырые части в splits/train_raw.parquet и splits/test_raw.parquet.
    Колонка времени остаётся, потому что препроцессинг сам строит календарные признаки.
    Возвращает пути к частям, месяц батча и подписи месяцев train и test.
    """
    SPLITS_DIR.mkdir(parents=True, exist_ok=True)
    dataset = pd.read_parquet(window_info['dataset_path'])
    dataset['transaction_time'] = pd.to_datetime(dataset['transaction_time'])
    periods = dataset['transaction_time'].dt.to_period('M')

    test_month = periods.max()
    train_months = sorted(periods.loc[periods < test_month].unique().astype(str))
    train = dataset[periods != test_month]
    test = dataset[periods == test_month]
    if len(train) == 0 or len(test) == 0:
        raise RuntimeError(
            f"Not enough data: train={len(train)}, test={len(test)}"
        )

    train_path = SPLITS_DIR / 'train_raw.parquet'
    test_path = SPLITS_DIR / 'test_raw.parquet'
    train.to_parquet(train_path, index=False)
    test.to_parquet(test_path, index=False)
    logger.info(
        "Train months: %s (%s lines)\n" \
        "Test_months: %s (%s lines)",
        train_months,
        len(train),
        test_month,
        len(test)
    )
    return {
        "train_raw_path": str(train_path),
        "test_raw_path": str(test_path),
        "batch_month": window_info['batch_month'],
        "train_months": ', '.join(train_months),
        "test_month": str(test_month),
        "dataset_path": window_info['dataset_path']
    }

