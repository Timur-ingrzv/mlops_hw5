from html import escape

from datetime import datetime, timezone

from pipeline.settings import REPORTS_DIR

# Нy html отчет конечно мощно, но пришлось завайбкодить
_CSS = """
body { font-family: -apple-system, Segoe UI, Roboto, sans-serif; margin: 24px; color:#111; }
h1 { margin: 0 0 6px 0; }
h2 { margin-top: 28px; }
.meta { color:#555; font-size: 13px; margin-bottom: 18px; }
.summary { display:flex; gap:16px; margin: 16px 0 24px 0; flex-wrap: wrap; }
.card { padding:12px 16px; border-radius:8px; background:#f4f6f8; min-width:130px; }
.card b { font-size: 22px; display:block; }
.pass { background:#e6f7ec; }
.fail { background:#fde8e8; }
table { border-collapse: collapse; width:100%; font-size:14px; }
th, td { border:1px solid #e1e4e8; padding:8px 10px; text-align:left; vertical-align:top; }
th { background:#f7f9fb; }
tr.sev-CRITICAL.fail td:first-child { border-left:4px solid #d1242f; }
tr.sev-WARNING.fail  td:first-child { border-left:4px solid #d4a72c; }
code { background:#f0f3f6; padding:1px 4px; border-radius:4px; }
details summary { cursor:pointer; color:#0366d6; }
"""


def _badge(passed: bool) -> str:
    return "✅ PASS" if passed else "❌ FAIL"


def render_html_report(results: list[dict], generated_at: str) -> str:
    total = len(results)
    failed = [r for r in results if not r["passed"]]
    critical_failed = [r for r in failed if r["severity"] == "CRITICAL"]

    rows = []
    for r in results:
        details = r.get("details") or {}
        details_html = ""
        if details:
            items = "".join(
                f"<li><code>{escape(str(k))}</code>: {escape(str(v))}</li>"
                for k, v in details.items()
            )
            details_html = f"<details><summary>details</summary><ul>{items}</ul></details>"

        row_class = f"sev-{r['severity']} {'fail' if not r['passed'] else ''}"
        rows.append(
            f'<tr class="{row_class}">'
            f"<td>{escape(str(r['name']))}</td>"
            f"<td>{escape(str(r['dataset']))}</td>"
            f"<td>{escape(str(r['severity']))}</td>"
            f"<td>{_badge(r['passed'])}</td>"
            f"<td>{escape(str(r['value']))}</td>"
            f"<td>{escape(str(r['threshold']))}</td>"
            f"<td>{escape(str(r['description']))}{details_html}</td>"
            f"</tr>"
        )

    return f"""<!doctype html>
    <html lang="ru"><head><meta charset="utf-8">
    <title>Data Quality Report — {escape(generated_at)}</title>
    <style>{_CSS}</style></head><body>
    <h1>Data Quality Report</h1>
    <div class="meta">Сгенерировано: {escape(generated_at)} UTC</div>

    <div class="summary">
      <div class="card"><b>{total}</b>всего правил</div>
     <div class="card pass"><b>{total - len(failed)}</b>пройдено</div>
     <div class="card fail"><b>{len(failed)}</b>провалено</div>
     <div class="card {'fail' if critical_failed else 'pass'}">
        <b>{len(critical_failed)}</b>CRITICAL провалено
    </div>
    </div>

    <h2>Результаты проверок</h2>
    <table>
    <thead><tr>
    <th>Правило</th><th>Датасет</th><th>Важность</th><th>Статус</th>
    <th>Значение</th><th>Порог</th><th>Описание</th>
    </tr></thead>
    <tbody>{''.join(rows)}</tbody>
    </table>
    </body></html>"""

def publish_report(results: list[dict]):
    """
    Рендерит HTML-отчёт о качестве данных и сохраняет его на диск.

    Формирует имя файла с UTC-таймстампом в формате
    dq_report_YYYY-MM-DDTHH-MM-SS.html, создаёт директорию
    REPORTS_DIR (если её ещё нет) и записывает туда отчёт,
    сгенерированный render_html_report.
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    REPORTS_DIR.mkdir(exist_ok=True, parents=True)
    report_path = REPORTS_DIR / f"dq_report_{ts}.html"
    report_path.write_text(
        render_html_report(results, generated_at=ts),
        encoding="utf-8",
    )
    return str(report_path)
