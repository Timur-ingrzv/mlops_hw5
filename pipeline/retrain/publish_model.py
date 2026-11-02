import json
import logging
import os
from datetime import date
from pathlib import Path

from airflow.exceptions import AirflowSkipException

from pipeline.db import update_training_state, get_conn
from pipeline.settings import CURRENT_DATASET, MODELS_DIR, F1_EPS

logger = logging.getLogger(__name__)

def read_prod_model_version() -> str | None:
    latest = MODELS_DIR / "latest.txt"
    if not latest.exists():
        return None
    return latest.read_text(encoding="utf-8").strip() or None

def load_metrics(version: str) -> dict | None:
    metrics_path = MODELS_DIR / f"my_classifier_{version}.metrics.json"
    if not metrics_path.exists():
        return None
    return json.loads(metrics_path.read_text(encoding="utf-8"))

def publish_model(train_info: dict) -> str:
    """Кладёт модель в общий каталог и только потом сдвигает указатель месяца.

    Переименовывает черновик .pkl в my_catboost_<version>.pkl, пишет метрики
    и атомарно обновляет latest.txt. Затем заменяет current.parquet новым окном
    и обновляет training_state. Повтор задачи не сдвигает окно второй раз,
    если pending-файл уже перенесён.
    Возвращает id опубликованной версии.
    """
    version = train_info["version"]
    staging_model = Path(train_info["staging_model"])
    final_model = MODELS_DIR / f"my_classifier_{version}.pkl"
    metrics_path = MODELS_DIR / f"my_classifier_{version}.metrics.json"

    metrics_path.write_text(
        json.dumps(
            {
                "version": version,
                "batch_month": train_info["batch_month"],
                "train_months": train_info["train_months"],
                "test_month": train_info["test_month"],
                "train_rows": train_info["train_rows"],
                "test_rows": train_info["test_rows"],
                "roc_auc": train_info["roc_auc"],
                "f1": train_info["f1"],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    
    if staging_model.exists():
        os.replace(staging_model, final_model)
    elif not final_model.exists():
        raise FileNotFoundError(f"No files for publishment: {staging_model}")
    staging_encoder = MODELS_DIR / f".staging_target_encoder_{version}.pkl"
    final_encoder = MODELS_DIR / f"target_encoder_{version}.pkl"
    if staging_encoder.exists():
        os.replace(staging_encoder, final_encoder)

    # Check model quality improvement
    production_version = read_prod_model_version()
    if production_version:
        production_metrics = load_metrics(production_version)
        if production_metrics:
            f1_prod = production_metrics['f1']
            f1_cur = train_info['f1']
            if f1_prod > f1_cur + F1_EPS:
                batch_month = date.fromisoformat(train_info["batch_month"])
                conn = get_conn()
                try:
                    update_training_state(conn, batch_month)
                finally:
                    conn.close()

                raise AirflowSkipException(
                    f"New model {version} is not better than {production_version} "
                    f"by F1: {f1_cur:.4f} < {f1_prod:.4f}. "
                    "Skip publishment."
                )

    latest_info = MODELS_DIR / "latest.txt"
    tmp = latest_info.with_suffix('.txt.tmp')
    tmp.write_text(f'{version}\n', encoding='utf-8')
    os.replace(tmp, latest_info)
    logger.info(
        'Model %s (%s) published',
        version, 
        final_model
    )

    pending_window = Path(train_info["dataset_path"])
    if pending_window.exists():
        os.replace(pending_window, CURRENT_DATASET)
        logger.info("Train window updated: %s", CURRENT_DATASET)
    else:
        logger.info("Window was already updated")
        return 

    batch_month = date.fromisoformat(train_info["batch_month"])
    conn = get_conn()
    try:
        update_training_state(conn, batch_month)
    finally:
        conn.close()
    return version