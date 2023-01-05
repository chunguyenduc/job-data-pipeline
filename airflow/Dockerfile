
ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.3.2-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

USER airflow
RUN pip install --user --no-cache-dir \
    requests[security] Scrapy 

COPY airflow.cfg /opt/airflow/
COPY webserver_config.py /opt/airflow/



