FROM bitnami/spark:3.1.3


ARG SPARK_VERSION="3.3.0"
ARG HADOOP_VERSION="2"

USER root
RUN rm -r /opt/bitnami/spark/jars && \
    curl --location https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | \
    tar --extract --gzip --strip=1 --directory /opt/bitnami/spark/ spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/
USER 1001   