"""
Общая фабрика Spark-сессий для всех джоб и ноутбуков.

Настройки подключения к S3 и нужные jar-пакеты собраны в одном месте,
чтобы стриминг, батч и ноутбуки не копировали их друг у друга.
Использование:
    from spark_session import get_spark
    spark = get_spark("имя-приложения")
"""

import os
from pathlib import Path

import pyspark
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Корень проекта: на уровень выше папки jobs/
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Читаем .env. Переменные, которые уже заданы в окружении (например,
# в контейнере Airflow), load_dotenv НЕ перезаписывает — они главнее.
load_dotenv(PROJECT_ROOT / ".env")


def get_spark(app_name: str) -> SparkSession:
    """Создает (или возвращает уже созданную) Spark-сессию с доступом к Kafka и S3."""

    spark_version = pyspark.__version__   # коннектор Kafka должен совпадать с версией Spark
    packages = ",".join([
        f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}",  # чтение из Kafka
        "org.apache.hadoop:hadoop-aws:3.3.4",                             # протокол s3a://
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",                     # AWS SDK для hadoop-aws
    ])

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))   # local-режим: все ядра компьютера
        .config("spark.jars.packages", packages)
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        # Все даты и время считаем в UTC одинаково и локально, и в Airflow
        .config("spark.sql.session.timeZone", "UTC")
        # Доступ к S3 (RustFS) через s3a://
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")        # адрес вида host/бакет
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")  # локально без HTTPS
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    # Spark очень многословен; оставляем в логах только предупреждения и ошибки
    spark.sparkContext.setLogLevel("WARN")
    return spark
