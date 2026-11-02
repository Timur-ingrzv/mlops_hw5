import logging
from pathlib import Path

import pandas as pd

from pipeline.db import copy_labeled_frame, get_conn

logger = logging.getLogger(__name__)


def append_batch(batch_info):
    batch_path = Path(batch_info['batch_path'])
    if not batch_path.exists():
        raise FileNotFoundError('File %s not exists', batch_path)
    data = pd.read_parquet(batch_path)
    conn = get_conn()
    try:
        copy_labeled_frame(conn, data)
    finally:
        conn.close()
    logger.info(
        "Month %s added to mart, len: %s",
        batch_info["batch_month"],
        len(data),
    )
    return {
        "batch_path": batch_info["batch_path"],
        "batch_month": batch_info["batch_month"],
        "rows": int(len(data)),
    }