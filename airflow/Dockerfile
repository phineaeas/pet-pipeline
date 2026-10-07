# Образ Airflow, который умеет запускать наши Spark-джобы.
# Берем официальный образ Airflow и добавляем в него:
#   - Java 17: Spark работает внутри Java-машины
#   - pyspark той же версии, что локально, и библиотеки наших джоб
FROM apache/airflow:3.3.1-python3.11

# Системные пакеты ставятся от root
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Python-пакеты — от пользователя airflow, как требует официальный образ.
# "apache-airflow==${AIRFLOW_VERSION}" не дает pip случайно обновить сам Airflow.
USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    pyspark==3.5.9 \
    clickhouse-connect \
    python-dotenv \
    boto3

# Папка для кэша jar-пакетов Spark. Создаем ее заранее от пользователя airflow,
# чтобы volume, подключенный сюда, принадлежал ему, а не root.
RUN mkdir -p /home/airflow/.ivy2
