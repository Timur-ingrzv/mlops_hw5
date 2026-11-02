from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pipeline.supply.append_batch import append_batch
from pipeline.supply.prune_old_data import prune_old_data
from pipeline.supply.sample_batch import sample_next_month

@dag(
    dag_id='supply_batch',
    description="Раз в 5 минут дописывает в витрину 50 тысяч строк следующего месяца",
    schedule="*/5 * * * *",
    max_active_runs=1,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mlops", "etl"],
    doc_md="""
    ### Поставка размеченного месяца
    
    Каждый запуск имитирует появление нового батча для обучения:

    1. Из всей витрины `labeled_transactions` случайно берутся 50 тысяч строк.
    2. Их `transaction_time` переносится в месяц, следующий за последним доступным.
    3. Батч дописывается в конец витрины.
    4. Наблюдения старше трёх лет относительно самого нового месяца удаляются.

    Исторические месяцы из CSV при этом не перезаписываются. DAG переобучения
    читает следующий месяц по своему указателю и доходит до этих батчей позже.
    """
)
def supply_labeled_batch():
    """Связывает выборку месяца, дозапись в витрину и очистку старых данных."""
    sampled = task(sample_next_month, task_id="sample_next_month")()
    appended = task(append_batch, task_id="append_labeled_batch")(sampled)
    task(prune_old_data, task_id="prune_old_months")(appended)

supply_labeled_batch()
