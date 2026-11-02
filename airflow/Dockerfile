FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libgomp1 \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Версии numpy/pandas зафиксированы: их обновление ломает Airflow 2.10.

RUN pip install --no-cache-dir \
    "numpy==1.26.4" \
    "pandas==2.1.4" \
    "scipy==1.14.1" \
    "joblib==1.4.2" \
    "scikit-learn==1.5.2" \
    "lightgbm==4.6.0" \
    "cloudpickle==2.2.1" \
    "joblib==1.4.2" \
    && python -c "import sklearn, pandas, pyarrow, numpy, cloudpickle; print(numpy.__version__, pandas.__version__, cloudpickle.__version__)"

USER root
COPY entrypoint.sh /custom-entrypoint.sh
RUN chmod +x /custom-entrypoint.sh

ENTRYPOINT ["/custom-entrypoint.sh"]