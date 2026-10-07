"""
Стриминг: читает события из Kafka и складывает их в S3 (RustFS) в Parquet.


  Каждые 30 секунд Spark заглядывает в топик events, забирает новые сообщения
  (с того места, где остановился в прошлый раз), превращает JSON в таблицу
  и дописывает ее в s3a://raw/events/, раскладывая по папкам-датам.
  Закладку "где остановился" Spark хранит в checkpoint-папке на диске.

Запуск (из корня проекта, в окружении pipeline):
    python jobs/stream_kafka_to_s3.py
Остановить: Ctrl+C. При следующем запуске стрим продолжит с того же места.
Пока стрим работает, его можно смотреть в Spark UI: http://localhost:4040
"""

import os

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

# Импорт get_spark заодно загружает .env, поэтому os.getenv ниже уже видит переменные
from spark_session import PROJECT_ROOT, get_spark

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "events")
RAW_PATH = os.getenv("RAW_EVENTS_PATH", "s3a://raw/events/")
# Checkpoint — локальная папка checkpoints/ в проекте (она в .gitignore)
CHECKPOINT = os.getenv("STREAM_CHECKPOINT", str(PROJECT_ROOT / "checkpoints" / "events_to_s3"))
TRIGGER = os.getenv("STREAM_TRIGGER", "30 seconds")

# Схема события: какие поля в JSON и какого они типа.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),   # строка "2026-10-05T14:32:10", превратим ниже
])


def main():
    spark = get_spark("stream-kafka-to-s3")

    # 1. ИСТОЧНИК. readStream - бесконечный поток вместо разового чтения.
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        # earliest действует только при ПЕРВОМ запуске. Дальше Spark берет
        # позицию из checkpoint и продолжает с нее.
        .option("startingOffsets", "earliest")
        .load()
    )

    # 2. ПРЕОБРАЗОВАНИЕ. Kafka отдает value в байтах: байты -> строка -> разбор JSON по схеме.
    events = (
        raw.select(
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
            # "координаты" сообщения в Kafka: пригодятся для отладки и поиска дублей
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .select("e.*", "kafka_partition", "kafka_offset")   # раскрываем поля события в колонки
        .where(F.col("event_id").isNotNull())               # выбрасываем сообщения, которые не разобрались
        .withColumn("event_ts", F.to_timestamp("event_ts")) # строка -> настоящий timestamp
        .withColumn("event_date", F.to_date("event_ts"))    # дата, по ней разложим файлы
    )

    # 3. ПРИЕМНИК. Пишем Parquet в S3, раскладывая по папкам event_date=YYYY-MM-DD.
    query = (
        events.writeStream
        .format("parquet")
        .option("path", RAW_PATH)
        .option("checkpointLocation", CHECKPOINT)
        .partitionBy("event_date")
        .outputMode("append")                      # только дописываем новое
        .trigger(processingTime=TRIGGER)           # микро-батч раз в 30 секунд
        .start()
    )

    print(f"Стрим запущен: Kafka '{TOPIC}' -> {RAW_PATH}, раз в {TRIGGER}.")
    print(f"Checkpoint: {CHECKPOINT}")
    print("Spark UI: http://localhost:4040  |  Остановить: Ctrl+C")

    try:
        query.awaitTermination()   # ждем бесконечно, пока стрим работает
    except KeyboardInterrupt:
        print("\nОстанавливаю стрим...")
        query.stop()


if __name__ == "__main__":
    main()
