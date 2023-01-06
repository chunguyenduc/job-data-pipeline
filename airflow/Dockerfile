
ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.5.0-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

USER airflow
COPY requirements.txt /opt/airflow/ 
RUN pip install --user --no-cache-dir \
    -r requirements.txt

COPY airflow.cfg /opt/airflow/
COPY webserver_config.py /opt/airflow/
COPY unittest.sh  /opt/airflow/



