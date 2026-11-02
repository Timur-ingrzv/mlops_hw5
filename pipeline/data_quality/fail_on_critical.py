from airflow.exceptions import AirflowFailException

def fail_on_crit(results: list[dict], report_path: str) -> None:
    """Выдает ошибку при выполнении DAG, если есть проваленные CRITICAL-правила."""
    critical_failed = [
        r for r in results if r["severity"] == "CRITICAL" and not r["passed"]
    ]
    if critical_failed:
        names = ", ".join(f"{r['name']}_{r['dataset']}" for r in critical_failed)
        raise AirflowFailException(
            f"{len(critical_failed)} CRITICAL DQ checks failed ({names}). "
            f"Report: {report_path}"
        )