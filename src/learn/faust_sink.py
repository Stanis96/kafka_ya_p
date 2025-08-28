from collections.abc import AsyncGenerator
from datetime import datetime
from typing import Any

import faust


# Создаем приложение Faust
app = faust.App(
    "simple-faust-app",
    broker="localhost:9094,localhost:9095,localhost:9096",
)

TOTAL_VALUE_THRESHOLD = 100


# Определяем модели данных
class UserEvent(faust.Record):  # noqa: D101
    user_id: str
    event_type: str
    timestamp: datetime
    value: float


class UserStats(faust.Record):  # noqa: D101
    total_events: int = 0
    last_event_time: datetime = datetime.now()
    total_value: float = 0.0


# Создаем таблицу для агрегации
user_stats_table = app.Table(
    "user_stats",
    default=UserStats,
    partitions=4,
    help="Статистика по пользователям",
)

# Создаем топики
user_events_topic = app.topic("user_events", value_type=UserEvent)
user_stats_topic = app.topic("user_stats_output", value_type=UserStats)


# Пример с фильтрацией в Sink
@app.agent(user_events_topic, sink=[user_stats_topic])
async def process_user_events_with_filter(events: faust.Stream[UserEvent]) -> AsyncGenerator[UserStats | Any, Any]:
    async for event in events:
        # Обновляем статистику
        stats = user_stats_table[event.user_id]
        if stats is None:
            stats = UserStats()
        stats.total_events += 1
        stats.last_event_time = event.timestamp
        stats.total_value += event.value
        user_stats_table[event.user_id] = stats

        # Отправляем статистику только если total_value > 100
        if stats.total_value > TOTAL_VALUE_THRESHOLD:
            yield stats


if __name__ == "__main__":
    app.main()
