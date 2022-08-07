
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
    apt-get -y install tzdata && \
    apt-get clean && \
    update-ca-certificates -f;

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

# ENV SPARK_HOME /usr/local/spark

# ARG SPARK_VERSION="3.1.2"
# ARG HADOOP_VERSION="2.7"
# # Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
# RUN cd "/tmp" && \
#         wget --no-verbose "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
#         tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
#         mkdir -p "${SPARK_HOME}/bin" && \
#         mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
#         cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/." "${SPARK_HOME}/bin/" && \
#         cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
#         rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# # Create SPARK_HOME env var
# RUN export SPARK_HOME
# ENV PATH $PATH:/usr/local/spark/bin

ENV TZ Asia/Ho_Chi_Minh

USER airflow
RUN pip install --user --no-cache-dir \
    requests[security] Scrapy hdfs && \
    PYSPARK_HADOOP_VERSION=2 pip install pyspark==3.3.0

COPY airflow.cfg /opt/airflow/
COPY webserver_config.py /opt/airflow/



