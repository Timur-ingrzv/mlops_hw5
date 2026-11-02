import logging
from datetime import date

import pandas as pd

from pipeline.settings import CURRENT_DATASET, PENDING_DATASET

logger = logging.getLogger(__name__)


def slide_training_window(batch_info: dict) -> dict:
    """Добавляет новый месяц к текущему окну и выбрасывает самый старый.

    Читает current.parquet и parquet батча из batch_info. Новое окно пишет
    в pending_window.parquet и не заменяет current.parquet: замена происходит
    только после публикации модели, чтобы повтор задачи не сдвинул окно дважды.
    Возвращает путь к новому окну и месяц добавленного батча.
    """
    if not CURRENT_DATASET.exists():
        raise FileNotFoundError(f"File {CURRENT_DATASET} doesn't exist")
    
    current = pd.read_parquet(CURRENT_DATASET)
    new_batch = pd.read_parquet(batch_info['batch_path'])
    current['transaction_time'] = pd.to_datetime(current['transaction_time'])
    new_batch['transaction_time'] = pd.to_datetime(new_batch['transaction_time'])

    new_dataset = pd.concat([current, new_batch], axis=0)
    periods = new_dataset['transaction_time'].dt.to_period('M')
    month_to_delete = periods.min()
    new_dataset = new_dataset[periods != month_to_delete].sort_values('transaction_time')
    new_dataset.to_parquet(PENDING_DATASET, index=False)
    logger.info(
        "New dataset: %s lines, period: %s - %s saved to %s",
        len(new_dataset),
        new_dataset.transaction_time.min(),
        new_dataset.transaction_time.max(),
        PENDING_DATASET
    )
    return {
        "dataset_path": str(PENDING_DATASET),
        "batch_month": batch_info['batch_month']
    }
