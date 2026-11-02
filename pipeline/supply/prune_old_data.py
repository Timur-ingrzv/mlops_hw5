import logging
from datetime import date

from dateutil.relativedelta import relativedelta

from pipeline.db import get_conn
from pipeline.settings import HISTORY_YEARS

logger = logging.getLogger(__name__)


def prune_old_data(batch_info):
    conn = get_conn()
    deleted = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT date_trunc('month', max(transaction_time))::date
                FROM labeled_transactions
                """
            )
            newest_month = cur.fetchone()[0]
            if newest_month is None:
                raise RuntimeError("labeled_transactions is empty")
            cutoff = newest_month - relativedelta(years=HISTORY_YEARS)
            query = """
                DELETE FROM labeled_transactions
                WHERE transaction_time < %s
            """
            cur.execute(query, (cutoff, ))
            deleted = cur.rowcount
        conn.commit()
    except Exception as e:
        logger.error(f'Error while pruning data: {e}')
    finally:
        conn.close()

    logger.info(
        "%s lines older %s deleted from labeled_transactions",
        deleted,
        cutoff
    )
    return {
        "batch_month": batch_info["batch_month"],
        "newest_month": newest_month.isoformat() if isinstance(newest_month, date) else str(newest_month),
        "cutoff": cutoff.isoformat(),
        "deleted_rows": int(deleted),
    }