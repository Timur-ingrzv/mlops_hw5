import logging
from datetime import datetime, timezone
import joblib
import pandas as pd
import numpy as np

from pipeline.settings import MODELS_DIR

logger = logging.getLogger(__name__)


def fit_model(preproc_info: dict) -> dict:
    """Обучает LGBMClassifier на train и считает ROC-AUC и F1 на свежем месяце.

    Читает обработанные parquet из preproc_info. Модель учится с ранней
    остановкой по test-месяцу и сохраняется во временный .pkl в каталоге моделей.
    В общий latest.txt файл ещё не публикуется.
    Возвращает версию, путь к черновику, метрики и путь к новому окну.
    """
    from lightgbm import LGBMClassifier, early_stopping
    from sklearn.metrics import f1_score, roc_auc_score

    train = pd.read_parquet(preproc_info['train_proc_path'])
    test = pd.read_parquet(preproc_info['test_proc_path'])
    model = LGBMClassifier(
        max_depth=8,
        learning_rate=0.05,
        n_estimators=80,
        colsample_bytree=0.9,
        reg_lambda=0.01
    )
    X_train = train.drop(columns='target')
    y_train = train['target']

    X_test = test.drop(columns='target')
    y_test = test['target']

    neg = int((y_train == 0).sum())
    pos = int((y_train == 1).sum())
    scale_pos_weight = neg / pos if pos > 0 else 1.0
    weights = np.where(y_train == 1, scale_pos_weight, 1)

    model.fit(
        X_train, 
        y_train,
        eval_set=[(X_test, y_test)],
        eval_metric='f1',
        callbacks=[early_stopping(stopping_rounds=20)],
        sample_weight=weights
    )
    probas = model.predict_proba(X_test)[:, 1]
    model_threshold = 0.8
    preds = (probas > model_threshold).astype(int)
    f1 = float(f1_score(y_test, preds, zero_division=0))
    if y_test.nunique() < 2:
        roc_auc = None
        logger.warning("Only one value of target in test, ROC-AUC not defined. F1=%.4f", f1)
    else:
        roc_auc = float(roc_auc_score(y_test, probas))
        logger.info(
            "Test %s: ROC-AUC=%.4f, F1=%.4f",
            preproc_info["test_month"],
            roc_auc,
            f1
        )

    version = preproc_info['version']
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    staging_model = MODELS_DIR / f".staging_model_{version}.pkl"
    joblib.dump(model, staging_model)
    logger.info(
        "New model: %s saved to buffer",
        version
    )
    return {
        "version": version,
        "staging_model": str(staging_model),
        "dataset_path": preproc_info["dataset_path"],
        "batch_month": preproc_info["batch_month"],
        "train_months": preproc_info["train_months"],
        "test_month": preproc_info["test_month"],
        "train_rows": int(len(X_train)),
        "test_rows": int(len(X_test)),
        "roc_auc": roc_auc,
        "f1": f1,
    }

    
