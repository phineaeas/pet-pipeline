"""
DAG events_pipeline: каждый день грузит вчерашние события из S3 в ClickHouse
и считает метрики.

Цепочка задач:
    get_target_date -> check_s3_partition -> load_day -> compute_metrics

  get_target_date     какой день обрабатываем (вчерашний относительно запуска)
  check_s3_partition  есть ли за этот день файлы в S3; если нет — пропускаем запуск
  load_day            запускает нашу Spark-джобу batch_s3_to_clickhouse.py
  compute_metrics     считает метрики дня в ClickHouse и пишет их в лог
"""

import os
from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task


@dag(
    dag_id="events_pipeline",
    description="S3 -> Spark -> ClickHouse, раз в день",
    # Запуск каждый день в полночь (UTC)
    schedule="@daily",
    # С какого момента Airflow начинает планировать запуски
    start_date=pendulum.datetime(2026, 10, 5, tz="UTC"),
    # catchup=True: если start_date в прошлом, Airflow сам создаст запуски
    # за все пропущенные дни (это и есть backfill)
    catchup=True,
    # Не больше одного запуска одновременно: Spark ест много памяти
    max_active_runs=1,
    default_args={
        "retries": 1,                          # при падении — одна повторная попытка
        "retry_delay": timedelta(minutes=1),   # через минуту
    },
    tags=["pet-pipeline"],
)
def events_pipeline():

    @task
    def get_target_date(**context) -> str:
        """Запуск в полночь обрабатывает вчерашний день: он уже закончился целиком."""
        # data_interval_end — момент, к которому относится запуск.
        # Для запуска вручную его может не быть, тогда берем время запуска.
        run_moment = context.get("data_interval_end") or context["dag_run"].run_after
        day = (run_moment - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Обрабатываем день {day}")
        return day   # возвращенное значение уходит в XCom и передается следующим задачам

    @task
    def check_s3_partition(day: str) -> str:
        """Проверяет, что в S3 есть папка events/event_date=<day>/ с файлами."""
        import boto3

        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT"],
            aws_access_key_id=os.environ["S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["S3_SECRET_KEY"],
            region_name="us-east-1",
        )
        resp = s3.list_objects_v2(Bucket="raw", Prefix=f"events/event_date={day}/", MaxKeys=1)
        if resp.get("KeyCount", 0) == 0:
            # Skip — не ошибка, а "нечего делать": задача станет розовой,
            # а следующие задачи этого запуска тоже пропустятся
            raise AirflowSkipException(f"В S3 нет данных за {day}")
        print(f"Данные за {day} в S3 есть")
        return day

    @task.bash
    def load_day(day: str) -> str:
        """Запускает Spark-джобу. Если она завершится с кодом 1, задача станет красной."""
        return f"python /opt/airflow/jobs/batch_s3_to_clickhouse.py --date {day}"

    @task
    def compute_metrics(day: str) -> dict:
        """Считает метрики дня по основной таблице events_agg."""
        import clickhouse_connect

        client = clickhouse_connect.get_client(
            host=os.environ["CH_HOST"],
            port=int(os.environ["CH_PORT"]),
            username=os.environ["CLICKHOUSE_USER"],
            password=os.environ["CLICKHOUSE_PASSWORD"],
        )
        # {day:Date} — параметр запроса: значение подставит сам ClickHouse
        row = client.query(
            """
            SELECT
                uniqMerge(users)    AS dau,
                sum(views)          AS total_views,
                sum(carts)          AS total_carts,
                sum(purchases)      AS total_purchases,
                sum(revenue)        AS total_revenue
            FROM pipeline.events_agg
            WHERE event_date = {day:Date}
            """,
            parameters={"day": day},
        ).result_rows[0]
        client.close()

        dau, views, carts, purchases, revenue = row
        metrics = {
            "day": day,
            "dau": int(dau),
            "views": int(views),
            "carts": int(carts),
            "purchases": int(purchases),
            "revenue": float(revenue),
            "view_to_cart_pct": round(100 * carts / views, 1) if views else None,
            "cart_to_purchase_pct": round(100 * purchases / carts, 1) if carts else None,
            "avg_check": round(float(revenue) / purchases, 2) if purchases else None,
        }
        for name, value in metrics.items():
            print(f"{name:>22}: {value}")
        return metrics   # метрики будут видны в UI на вкладке XCom

    # Порядок задач. Передача значения из одной задачи в другую
    # одновременно задает зависимость между ними.
    day = get_target_date()
    checked_day = check_s3_partition(day)
    load_day(checked_day) >> compute_metrics(checked_day)


events_pipeline()
