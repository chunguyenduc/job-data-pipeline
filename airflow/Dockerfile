
ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.3.2-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

USER root

# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y gnupg2 && \
    apt-get install -y libsasl2-dev libsasl2-2 libsasl2-modules-gssapi-mit && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB9B1D8886F44E2A && \
    add-apt-repository "deb http://security.debian.org/debian-security stretch/updates main" && \ 
    apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    java -version $$ \
    javac -version

# Fix certificate issues
RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get -y install tzdata && \
    apt-get clean && \
    update-ca-certificates -f;

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

ENV TZ Asia/Ho_Chi_Minh

USER airflow
RUN pip install --user --no-cache-dir \
    requests[security] Scrapy hdfs pyhive thrift thrift-sasl && \
    PYSPARK_HADOOP_VERSION=2 pip install pyspark==3.3.0

COPY airflow.cfg /opt/airflow/
COPY webserver_config.py /opt/airflow/



