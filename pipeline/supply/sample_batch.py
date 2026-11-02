import psycopg2
import calendar
import pandas as pd
from pipeline.db import get_conn, COLUMNS
from pipeline.settings import SAMPLE_SIZE, SUPPLY_BATCH, TRAINING_DIR
import logging 

logger = logging.getLogger(__name__)


def next_month_start(max_time: pd.Timestamp) -> pd.Timestamp:
    """Возвращает первое число месяца, следующего за месяцем max_time."""
    return (pd.Timestamp(max_time).to_period("M") + 1).to_timestamp()

def shift_into_month(frame: pd.DataFrame, month_start: pd.Timestamp) -> pd.DataFrame:
    """Переписывает transaction_time в заданный месяц, сохраняя день, час и минуту.

    Если в целевом месяце нет такого дня, день обрезается до последнего числа.
    Секунды обнуляются. Исходный frame не меняется.
    """
    shifted = frame.copy()
    times = pd.to_datetime(shifted["transaction_time"])
    year = int(month_start.year)
    month = int(month_start.month)
    last_day = calendar.monthrange(year, month)[1]
    shifted["transaction_time"] = pd.to_datetime(
        {
            "year": year,
            "month": month,
            "day": times.dt.day.clip(upper=last_day),
            "hour": times.dt.hour,
            "minute": times.dt.minute,
        }
    )
    return shifted

def sample_next_month():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT max(transaction_time), count(*) FROM labeled_transactions")
            max_time, row_count = cur.fetchone()
        if max_time is None or row_count == 0:
            raise RuntimeError("Mart labeled_transactions is empty, wait until data_loader finishes")

        sample_size = min(SAMPLE_SIZE, row_count)
        columns = ", ".join(COLUMNS)
        data = pd.read_sql(
            f"""
            SELECT {columns}
            FROM labeled_transactions
            ORDER BY random()
            LIMIT %s
            """,
            conn,
            params=(sample_size,),
        )
    finally:
        conn.close()

    month_start = next_month_start(max_time)
    batch = shift_into_month(data, month_start)
    TRAINING_DIR.mkdir(exist_ok=True, parents=True)
    batch.to_parquet(SUPPLY_BATCH, index=False)
    logger.info(
        "Batch %s: %s lines of %s, time %s — %s",
        month_start.strftime("%Y-%m"),
        len(batch),
        row_count,
        batch["transaction_time"].min(),
        batch["transaction_time"].max(),
    )
    return {
        "batch_path": str(SUPPLY_BATCH),
        "batch_month": month_start.date().isoformat(),
        "rows": int(len(batch)),
    }