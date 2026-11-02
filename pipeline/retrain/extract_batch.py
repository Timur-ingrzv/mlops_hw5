import logging
from dateutil.relativedelta import relativedelta
import pandas as pd
from airflow.exceptions import AirflowSkipException
from pathlib import Path

from pipeline.db import get_conn, COLUMNS, get_state
from pipeline.settings import TRAINING_DIR

logger = logging.getLogger(__name__)


def extract_batch():
    try:
        conn = get_conn()
        state = get_state(conn)
        batch_start = state['next_batch_month']
        batch_end = batch_start + relativedelta(months=1)
        logger.info(
            "Current period: %s - %s, extract: %s",
            state['window_start'],
            state['window_end'],
            batch_start
        )
        cols = ', '.join(COLUMNS)
        query = f"""
        SELECT {cols} 
        FROM labeled_transactions
        WHERE transaction_time >= %s AND
        transaction_time < %s
        ORDER BY transaction_time
        """
        data = pd.read_sql(
            query,
            con=conn,
            params=(batch_start, batch_end)
        )
    finally:
        conn.close()

    if len(data) == 0:
        raise AirflowSkipException(
            f"No data for month {batch_start}"
        )
    batch_path = TRAINING_DIR / f"{batch_start.strftime('%Y_%m')}.parquet"
    data.to_parquet(batch_path, index=False)
    logger.info(
        "Extracted batch: %s lines, period: %s - %s",
        len(data),
        data.transaction_time.min(),
        data.transaction_time.max()
    )
    return {
        "batch_path": str(batch_path),
        "batch_month": batch_start.isoformat()
    }
