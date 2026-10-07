"""
Батч: берет один день сырых событий из S3, размножает их и грузит в ClickHouse.

Что происходит по шагам:
  1. Spark читает из s3a://raw/events/ только папку event_date=<дата>.
  2. Размножает события: оригинал + (N-1) копий с новыми event_id,
     другими user_id и немного сдвинутым временем внутри того же дня.
  3. Удаляет этот день в ClickHouse (в events_raw и events_agg), чтобы
     повторный запуск не создал дублей. 
  4. Пишет события в events_raw. Матвьюха сама досчитает events_agg.
  5. Сверяет количество строк. Если не сошлось, завершается с ошибкой,
     и Airflow пометит задачу красной.

Запуск (из корня проекта, в окружении pipeline):
    python jobs/batch_s3_to_clickhouse.py --date 2026-10-05
    python jobs/batch_s3_to_clickhouse.py --date 2026-10-05 --multiplier 5
"""

import argparse
import os
import sys
from datetime import datetime
from functools import reduce

import clickhouse_connect
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# Импорт get_spark заодно загружает .env
from spark_session import get_spark

RAW_PATH = os.getenv("RAW_EVENTS_PATH", "s3a://raw/events/")

CH_SETTINGS = {
    "host": os.getenv("CH_HOST", "localhost"),
    "port": int(os.getenv("CH_PORT", "8123")),
    "username": os.environ["CLICKHOUSE_USER"],
    "password": os.environ["CLICKHOUSE_PASSWORD"],
}
CH_TABLE = "pipeline.events_raw"
CH_COLUMNS = ["event_id", "user_id", "product_id", "category",
              "price", "event_type", "event_ts", "event_date"]

# Генератор выдает user_id от 1 до 1000. Копия №k получает user_id + k * 1000,
# поэтому у каждой копии свои, непересекающиеся пользователи.
USER_ID_STEP = 1000


def parse_args():
    parser = argparse.ArgumentParser(description="Загрузка одного дня из S3 в ClickHouse")
    parser.add_argument("--date", required=True, help="день в формате YYYY-MM-DD")
    parser.add_argument("--multiplier", type=int, default=int(os.getenv("AUGMENT_MULTIPLIER", "10")),
                        help="во сколько раз размножить данные (1 — без размножения)")
    return parser.parse_args()


def augment(df: DataFrame, multiplier: int) -> DataFrame:
    """Возвращает оригинал + (multiplier - 1) измененных копий, склеенных в один DataFrame."""

    # Новое время = то же время суток + случайный сдвиг до часа, но "по кругу"
    # внутри суток (pmod — остаток от деления), чтобы дата не поменялась.
    shifted_ts = F.expr("""
        timestamp_seconds(
            unix_timestamp(cast(event_date AS timestamp))
            + pmod(unix_timestamp(event_ts) - unix_timestamp(cast(event_date AS timestamp))
                   + cast(rand() * 3600 AS int), 86400)
        )
    """)

    copies = [df]   # копия №0 оригинальные события без изменений
    for k in range(1, multiplier):
        copies.append(
            df.withColumn("event_id", F.expr("uuid()"))                         # новый уникальный id
              .withColumn("user_id", F.col("user_id") + F.lit(k * USER_ID_STEP))  # другие пользователи
              .withColumn("event_ts", shifted_ts)                                # чуть другое время
        )
    # reduce склеивает список DataFrame'ов попарно: ((df0 ∪ df1) ∪ df2) ∪ ...
    return reduce(DataFrame.unionByName, copies)


def write_partition(rows):
    """
    Пишет одну часть (партицию) DataFrame в ClickHouse.
    Spark вызывает эту функцию отдельно для каждой части, в отдельных
    Python-процессах, поэтому подключение создается прямо здесь.
    """
    client = clickhouse_connect.get_client(**CH_SETTINGS)
    batch = []
    for row in rows:
        batch.append([row[c] for c in CH_COLUMNS])
        if len(batch) >= 100_000:
            client.insert(CH_TABLE, batch, column_names=CH_COLUMNS)
            batch = []
    if batch:
        client.insert(CH_TABLE, batch, column_names=CH_COLUMNS)
    client.close()


def main():
    args = parse_args()
    # Проверяем формат даты заранее: дата попадет в SQL-запрос
    day = datetime.strptime(args.date, "%Y-%m-%d").date().isoformat()

    spark = get_spark(f"batch-s3-to-clickhouse-{day}")

    # 1. Читаем один день. Фильтр по колонке-папке event_date Spark превращает
    #    в чтение только одной папки в S3 (partition pruning), остальные не трогает.
    raw = spark.read.parquet(RAW_PATH).where(F.col("event_date") == F.lit(day))
    raw_count = raw.count()
    if raw_count == 0:
        print(f"В {RAW_PATH} нет событий за {day}. Нечего грузить.")
        sys.exit(1)

    # 2. Размножаем и готовим колонки под таблицу ClickHouse
    events = (
        augment(raw, args.multiplier)
        .select(
            "event_id", "user_id", "product_id", "category",
            F.col("price").cast(DecimalType(10, 2)).alias("price"), 
            "event_type",
            # Время передаем числом секунд с 1970 года: так оно не исказится
            # из-за часовых поясов по дороге из Spark в Python и в ClickHouse
            F.unix_timestamp("event_ts").alias("event_ts"),
            "event_date",
        )
        .repartition(4)   # 4 части = 4 параллельные вставки в ClickHouse
        .cache()          # запоминаем результат, чтобы count и запись видели одни и те же строки
    )
    expected = events.count()
    print(f"{day}: в S3 {raw_count} событий, после размножения x{args.multiplier}: {expected}")

    # 3. Удаляем день целиком в обеих таблицах
    client = clickhouse_connect.get_client(**CH_SETTINGS)
    client.command(f"ALTER TABLE pipeline.events_raw DROP PARTITION '{day}'")
    client.command(f"ALTER TABLE pipeline.events_agg DROP PARTITION '{day}'")

    # 4. Пишем в ClickHouse. Матвьюха events_mv на каждую вставку досчитает events_agg.
    events.foreachPartition(write_partition)

    # 5. Сверка: в raw столько строк, сколько отправили, и в agg то же число событий
    loaded = client.query(
        f"SELECT count() FROM pipeline.events_raw WHERE event_date = '{day}'"
    ).result_rows[0][0]
    in_agg = client.query(
        f"SELECT sum(views) + sum(carts) + sum(purchases) FROM pipeline.events_agg WHERE event_date = '{day}'"
    ).result_rows[0][0]
    client.close()
    spark.stop()

    print(f"{day}: в events_raw {loaded}, в events_agg {in_agg} (ожидалось {expected})")
    if loaded != expected or in_agg != expected:
        print("Количество не сошлось!")
        sys.exit(1)
    print("Готово, всё сошлось.")


if __name__ == "__main__":
    main()
