import pandas as pd
import numpy as np
from airflow.exceptions import AirflowSkipException

from pipeline.settings import DQ_RULES, SPLITS_DIR


def rule_class_balance(df: pd.DataFrame, dataset: str) -> dict:
    ratio = float(df["target"].mean()) if "target" in df and len(df) else 0.0
    return {
        "name": "target_balance",
        "description": "Доля положительного класса в датасете > порога",
        "severity": "CRITICAL",
        "passed": ratio > DQ_RULES["target_balance_min"],
        "value": round(ratio, 5),
        "threshold": DQ_RULES["target_balance_min"],
        "dataset": dataset,
        "details": None
    }

def rule_missing_values(df: pd.DataFrame, dataset: str) -> dict:
    if df.empty:
        ratio, worst_col = 1.0, None
    else:
        ratios = df.isna().mean()
        worst_col = ratios.idxmax()
        ratio = float(ratios.max())
    return {
        "name": "missing_values",
        "description": "Максимальная доля пропусков в любой колонке < порога",
        "severity": "CRITICAL",
        "passed": ratio < DQ_RULES["missing_ratio_max"],
        "value": round(ratio, 5),
        "threshold": DQ_RULES["missing_ratio_max"],
        "dataset": dataset,
        "details": {"worst_column": str(worst_col)},
    }

def rule_amount_range(df: pd.DataFrame, dataset: str) -> dict:
    col = "amount"
    negatives = int((df[col] < DQ_RULES["amount_min"]).sum())
    return {
        "name": "amount_range",
        "description": "Нет отрицательных сумм транзакций",
        "severity": "CRITICAL",
        "passed": negatives == 0,
        "value": negatives,
        "threshold": 0,
        "dataset": dataset,
        "details": {"column": col},
    }

def rule_amount_outliers(df: pd.DataFrame, dataset: str) -> dict:
    col = "amount"
    z = np.abs((df[col] - df[col].mean()) / (df[col].std(ddof=0) + 1e-9))
    ratio = float((z > DQ_RULES["amount_zscore_max"]).mean())
    return {
        "name": "amount_outliers",
        "description": f"Доля выбросов по amount (|z|>{DQ_RULES['amount_zscore_max']}) < {DQ_RULES['amount_outlier_ratio_max']:.0%}",
        "severity": "WARNING",
        "passed": ratio < DQ_RULES["amount_outlier_ratio_max"],
        "value": round(ratio, 5),
        "threshold": DQ_RULES["amount_outlier_ratio_max"],
        "dataset": dataset,
        "details": {"column": col},
    }

def rule_duplicates(df: pd.DataFrame, dataset: str) -> dict:
    ratio = float(df.duplicated().mean()) if not df.empty else 0.0
    return {
        "name": "duplicates",
        "description": "Доля дублей по transaction_id < порога",
        "severity": "CRITICAL",
        "passed": ratio < DQ_RULES["duplicate_ratio_max"],
        "value": round(ratio, 5),
        "threshold": DQ_RULES["duplicate_ratio_max"],
        "dataset": dataset,
        "details": {"key": 'full_row_duplicates'},
    }

def rule_constant_features(df: pd.DataFrame, dataset: str) -> dict:
    feature_cols = [c for c in df.columns if c != "target"]
    if not feature_cols or df.empty:
        bad = []
    else:
        stds = df[feature_cols].std(numeric_only=True, ddof=0).fillna(0.0)
        bad = stds[stds < DQ_RULES["feature_std_min"]].index.tolist()
    return {
        "name": "constant_features",
        "description": f"Нет константных признаков (std < {DQ_RULES['feature_std_min']})",
        "severity": "WARNING",
        "passed": len(bad) == 0,
        "value": len(bad),
        "threshold": 0,
        "dataset": dataset,
        "details": {"columns": [str(c) for c in bad[:20]]},
    }

def run_checks():
    """
    Запускает набор Data Quality проверок над train и test датасетами.

    Читает препроцессированные датасеты из ``SPLITS_DIR``
    (``train_proc.parquet`` и ``test_proc.parquet``) и применяет к каждому
    из них все правила из фиксированного списка (баланс классов, пропуски,
    диапазон и выбросы по amount, дубликаты, константные признаки).
    """
    train_path = SPLITS_DIR / "train_proc.parquet"
    test_path = SPLITS_DIR / "test_proc.parquet"
    if not train_path.exists():
        raise AirflowSkipException(f"Dataset {train_path} not exist")
    if not test_path.exists():
        raise AirflowSkipException(f"Dataset {test_path} not exist")
    
    datasets = {
        "train":  pd.read_parquet(train_path),
        "test":  pd.read_parquet(test_path)
    }
    rules = (
        rule_class_balance,
        rule_missing_values,
        rule_amount_range,
        rule_amount_outliers,
        rule_duplicates,
        rule_constant_features
    )
    results: list[dict] = []
    for dataset, df in datasets.items():
        for rule in rules:
            try:
                results.append(rule(df, dataset))
            except Exception as e: 
                results.append({
                    "name": rule.__name__,
                    "description": "Rule raised an exception",
                    "severity": "CRITICAL",
                    "passed": False,
                    "value": None,
                    "threshold": None,
                    "dataset": dataset,
                    "details": {"error": str(e)},
                })
    return results
