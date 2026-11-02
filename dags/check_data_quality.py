from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.data_quality.run_dq_checks import run_checks
from pipeline.data_quality.publish_report import publish_report
from pipeline.data_quality.fail_on_critical import fail_on_crit


@dag(
    dag_id="data_quality_checks",
    description="Проверяет датасет последней модели на соответствие DQ-требованиям и публикует HTML-отчёт рядом в reports/",
    schedule=None,                
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"retries": 0}, 
    tags=["dq", "mlops", "lgbm"],
    doc_md="""
### Проверка Data Quality

Проверяет parquet-датасеты, использованные последней моделью, прогоняет
набор DQ-правил и публикует HTML-отчёт в `reports/` рядом с `models/`.

Если есть проваленные CRITICAL-правила — DAG падает; HTML-отчёт всё равно
публикуется. WARNING-правила не блокируют, только фиксируются в отчёте.

Запускается после завершения Dag `retrain_model`, после `publish_model`. 
    """
)
def data_quality_checks():
    results = task(run_checks, task_id="run_dq_checks")()
    report = task(publish_report, task_id="publish_report")(results)
    task(fail_on_crit, task_id="dq_critical_error")(results, report)
    
data_quality_checks()
