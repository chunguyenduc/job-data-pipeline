
ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.3.2-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

USER root

# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y gnupg2 && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB9B1D8886F44E2A && \
    add-apt-repository "deb http://security.debian.org/debian-security stretch/updates main" && \ 
    apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    java -version $$ \
    javac -version
    
# Fix certificate issues
RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

USER airflow
RUN pip install --user --no-cache-dir \
    Scrapy pyspark hdfs 

COPY default-airflow.cfg opt/airflow/airflow.cfg
COPY webserver_config.py opt/airflow/webserver_config.py



