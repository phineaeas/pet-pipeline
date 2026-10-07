"""
Producer: непрерывно отправляет синтетические события в Kafka.

Запуск (из корня проекта, в окружении pipeline):
    python jobs/producer.py                # бесконечно, 20 событий в секунду
    python jobs/producer.py --rate 5       # 5 событий в секунду
    python jobs/producer.py --count 30     # отправить 30 событий и остановиться
Остановить бесконечный режим: Ctrl+C.
"""

import argparse                     # разбор параметров командной строки (--rate, --count)
import json
import os
import time
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

# Наш генератор из jobs/events.py. Импорт работает, потому что скрипт
# запускается из папки jobs/, и Python ищет модули в первую очередь там.
from events import generate_event

# .env лежит в корне проекта: на уровень выше папки jobs/
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# Адрес и топик — из окружения, с запасными значениями для локального запуска
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "events")


def parse_args():
    """Описываем, какие параметры можно передать скрипту в командной строке."""
    parser = argparse.ArgumentParser(description="Отправка синтетических событий в Kafka")
    parser.add_argument("--rate", type=float, default=20, help="событий в секунду")
    parser.add_argument("--count", type=int, default=0, help="сколько отправить; 0 — бесконечно")
    return parser.parse_args()


# Счетчики результатов доставки. Это словарь, а не две отдельные переменные,
# чтобы функция on_delivery ниже могла их менять.
stats = {"delivered": 0, "failed": 0}


def on_delivery(err, msg):
    """Kafka вызывает эту функцию, когда узнает судьбу сообщения: доставлено или нет."""
    if err is not None:
        stats["failed"] += 1
        print(f"Ошибка доставки: {err}")
    else:
        stats["delivered"] += 1


def main():
    args = parse_args()

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        # acks=all: сообщение считается записанным, только когда брокер его сохранил
        "acks": "all",
        # Идемпотентность: если сеть моргнула и producer отправил сообщение повторно,
        # Kafka распознает повтор и не запишет дубль
        "enable.idempotence": True,
        # Копим сообщения до 50 мс и отправляем пачкой — так эффективнее, чем по одному
        "linger.ms": 50,
    })

    pause = 1 / args.rate      # пауза между событиями в секундах
    sent = 0
    print(f"Отправляю в топик '{TOPIC}' на {KAFKA_BOOTSTRAP}, {args.rate} событий/сек. Ctrl+C — стоп.")

    try:
        # Крутимся бесконечно (count == 0) или пока не отправим нужное количество
        while args.count == 0 or sent < args.count:
            event = generate_event()
            producer.produce(
                TOPIC,
                # КЛЮЧ = user_id. Kafka по хешу ключа выбирает партицию, поэтому
                # все события одного пользователя попадают в одну партицию
                # и сохраняют порядок между собой.
                key=str(event["user_id"]).encode("utf-8"),
                # ЗНАЧЕНИЕ = само событие. Kafka хранит байты: dict -> JSON -> байты
                value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
                on_delivery=on_delivery,
            )
            sent += 1

            # produce только кладет сообщение в очередь внутри producer'а.
            # poll(0) дает ему обработать ответы брокера и вызвать on_delivery.
            producer.poll(0)

            if sent % 100 == 0:
                print(f"Отправлено: {sent}, доставлено: {stats['delivered']}, ошибок: {stats['failed']}")
            time.sleep(pause)

    except KeyboardInterrupt:
        # Ctrl+C прерывает цикл — выходим аккуратно, ничего не потеряв
        print("\nОстанавливаюсь...")

    finally:
        # finally выполняется всегда. flush дожидается доставки всего,
        # что еще лежит в очереди producer'а
        producer.flush(10)
        print(f"Итого отправлено: {sent}, доставлено: {stats['delivered']}, ошибок: {stats['failed']}")


if __name__ == "__main__":
    main()
