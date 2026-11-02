from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from pipeline.retrain.extract_batch import extract_batch
from pipeline.retrain.fit_model import fit_model
from pipeline.retrain.preprocess_data import preprocess_dataset
from pipeline.retrain.publish_model import publish_model
from pipeline.retrain.slide_window import slide_training_window
from pipeline.retrain.time_train_split import time_train_test_split


@dag(
    dag_id="retrain_model",
    description="Раз в 5 минут сдвигает 3-месячное окно и переобучает LGBMClassifier и encoder",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mlops", "lgbm"],
    doc_md="""
### Переобучение модели

Каждый запуск имитирует новый месяц в проде:
1. Из витрины `labeled_transactions` берётся следующий календарный месяц.
2. Месяц добавляется в датасет из трёх месяцев, самый старый месяц выбрасывается.
3. Train — два более ранних месяца, test — самый свежий.
4. Препроцессинг — `run_preproc` из `fraud_detector/src`.
5. LGBMClassifier обучается, бинарник публикуется в общий каталог моделей.

Указатель месяца в `training_state` сдвигается только после публикации модели.
Если очередного месяца в витрине ещё нет, задачи помечаются как skipped.
Новые месяцы после исторических данных дописывает DAG `supply_labeled_batch`.
    """,
)
def retrain_catboost():
    """Связывает извлечение месяца, сдвиг окна, split, препроцессинг, обучение и публикацию."""
    extracted = task(extract_batch, task_id="extract_month_batch")()
    slid = task(slide_training_window, task_id="slide_training_window")(extracted)
    split = task(time_train_test_split, task_id="time_train_test_split")(slid)
    processed = task(preprocess_dataset, task_id="preprocess_dataset")(split)
    trained = task(fit_model, task_id="fit_lgbm")(processed)
    published = task(publish_model, task_id="publish_model")(trained)

    dq_trigger = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality_checks",
        trigger_rule="none_failed"
    )
    published >> dq_trigger

retrain_catboost()