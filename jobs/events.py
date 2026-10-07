"""
Генератор событий интернет магазина

Одно событие = одно действие пользователя, эту функцию потом будет
использовать producer, который отправляет события в Kafka.
"""

import json                                  
import random                                
import uuid                                  
from datetime import datetime, timedelta     



CATEGORIES = ["electronics", "clothing", "books", "home", "sport"]

# Типы событий и их веса: 70% просмотров, 20% добавлений в корзину,
# 10% покупок
EVENT_TYPES = ["view", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [70, 20, 10]

# Насколько далеко в прошлое разбрасываем события: 3 дня в секундах.
# Нужно, чтобы в S3 появилось несколько дневных партиций,
# а DAG в Airflow отработал по одному запуску на каждый день
MAX_AGE_SECONDS = 3 * 24 * 60 * 60


def generate_event() -> dict:
    """Создает одно случайное событие и возвращает его как словарь (dict)."""


    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    seconds_ago = random.randint(0, MAX_AGE_SECONDS)
    event_ts = datetime.now() - timedelta(seconds=seconds_ago)

    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1000),
        "product_id": random.randint(1, 200),
        "category": random.choice(CATEGORIES),
        "price": round(random.uniform(100, 5000), 2),
        "event_type": event_type,
        "event_ts": event_ts.isoformat(timespec="seconds"),
    }
    return event


# Этот блок выполняется, только когда файл запускают напрямую:
#     python jobs/events.py
# Когда producer сделает "from events import generate_event",
# этот код НЕ выполнится, подтянется только функция.
if __name__ == "__main__":
    for _ in range(5):        # повторить 5 раз; "_" — счетчик, который нам не нужен
        event = generate_event()
        # json.dumps превращает словарь в строку JSON.
        # ensure_ascii=False — чтобы русские буквы, если появятся,
        # печатались как есть, а не как \u0434\u043e...
        print(json.dumps(event, ensure_ascii=False))