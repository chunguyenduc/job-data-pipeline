# FROM python:3.7-stretch

# # Never prompt the user for choices on installation/configuration of packages
# ENV DEBIAN_FRONTEND noninteractive
# ENV TERM linux

# # Airflow
# ARG AIRFLOW_VERSION=2.3.2
# ARG AIRFLOW_USER_HOME=/usr/local/airflow
# ARG AIRFLOW_DEPS=""
# ARG PYTHON_DEPS="Scrapy hdfs"
# ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
# ENV _AIRFLOW_WWW_USER_USERNAME="airflow"
# ENV _AIRFLOW_WWW_USER_PASSWORD="airflow"
# ARG SPARK_VERSION="3.1.2"
# ARG HADOOP_VERSION="2.7.4"


# # Define en_US.
# ENV LANGUAGE en_US.UTF-8
# ENV LANG en_US.UTF-8
# ENV LC_ALL en_US.UTF-8
# ENV LC_CTYPE en_US.UTF-8
# ENV LC_MESSAGES en_US.UTF-8


# RUN set -ex \
#     && buildDeps=' \
#     freetds-dev \
#     libkrb5-dev \
#     libsasl2-dev \
#     libssl-dev \
#     libffi-dev \
#     libpq-dev \
#     git \
#     ' \
#     && apt-get update -yqq \
#     && apt-get upgrade -yqq \
#     && apt-get install -yqq --no-install-recommends \
#     $buildDeps \
#     freetds-bin \
#     build-essential \
#     default-libmysqlclient-dev \
#     apt-utils \
#     curl \
#     rsync \
#     netcat \
#     locales \
#     && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
#     && locale-gen \
#     && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
#     && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow \
#     && pip install -U pip setuptools wheel \
#     && pip install pytz \
#     && pip install pyOpenSSL \
#     && pip install ndg-httpsclient \
#     && pip install pyasn1 \
#     && PYSPARK_HADOOP_VERSION=2 pip install pyspark -v  \
#     && pip install apache-airflow[crypto,celery,postgres,hive,jdbc,mysql,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
#     && pip install 'redis' \
#     && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi \
#     && apt-get purge --auto-remove -yqq $buildDeps \
#     && apt-get autoremove -yqq --purge \
#     && apt-get clean \
#     && rm -rf \
#     /var/lib/apt/lists/* \
#     /tmp/* \
#     /var/tmp/* \
#     /usr/share/man \
#     /usr/share/doc \
#     /usr/share/doc-base

# ## Begin JAVA installation
# ###############################
# # Java is required in order to spark-submit work
# # Install OpenJDK-8
# RUN apt-get update && \
#     apt-get install -y software-properties-common && \
#     apt-get install -y gnupg2 && \
#     apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB9B1D8886F44E2A && \
#     add-apt-repository "deb http://security.debian.org/debian-security stretch/updates main" && \ 
#     apt-get update && \
#     apt-get install -y openjdk-8-jdk && \
#     pip freeze && \
#     java -version $$ \
#     javac -version

# # Setup JAVA_HOME 
# ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
# RUN export JAVA_HOME
# #####################

# # Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
# RUN cd "/tmp" && \
#     wget --no-verbose "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
#     tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
#     mkdir -p "${SPARK_HOME}/bin" && \
#     mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
#     cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/." "${SPARK_HOME}/bin/" && \
#     cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
#     rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# # Create SPARK_HOME env var
# RUN export SPARK_HOME
# ENV PATH $PATH:/usr/local/spark/bin
# ###############################


# COPY entrypoint.sh /entrypoint.sh
# RUN chmod a+x /entrypoint.sh
# ADD webserver_config.py ${AIRFLOW_USER_HOME}
# COPY airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg


# RUN chown -R airflow: ${AIRFLOW_USER_HOME}

# EXPOSE 8080 5555 8793

# USER airflow
# WORKDIR ${AIRFLOW_USER_HOME}
# ENTRYPOINT ["/entrypoint.sh"]
# CMD ["webserver"]
ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.0.0-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}
RUN pip install --user --no-cache-dir \
    Scrapy