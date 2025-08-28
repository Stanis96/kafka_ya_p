from datetime import datetime, timedelta

import faust


# Создаем приложение Faust
app = faust.App(
    "simple-faust-app",
    broker="localhost:9094,localhost:9095,localhost:9096",
)


# Определяем модели данных
class UserEvent(faust.Record):  # noqa:D101
    user_id: str
    event_type: str
    timestamp: datetime
    value: float


class UserStats(faust.Record):  # noqa:D101
    total_events: int = 0
    last_event_time: datetime = datetime.now()
    total_value: float = 0.0


# Создаем таблицы
user_stats_table = app.Table(
    "user_stats",
    default=UserStats,
    partitions=4,
    help="Статистика по пользователям",
)


# Создаем временное окно для агрегации
windowed_stats = app.Table(
    "windowed_stats",
    default=UserStats,
    partitions=4,
    help="Статистика по временным окнам",
).hopping(
    size=timedelta(minutes=5),
    step=timedelta(minutes=1),
    expires=timedelta(hours=1),
)


# Топик для входящих событий
user_events_topic = app.topic("user_events", value_type=UserEvent)


@app.agent(user_events_topic)
async def process_user_events(events: faust.Stream[UserEvent]) -> None:
    async for event in events:
        # Обновляем общую статистику
        stats = user_stats_table[event.user_id]
        if stats is None:
            stats = UserStats()
        stats.total_events += 1
        stats.last_event_time = event.timestamp
        stats.total_value += event.value
        user_stats_table[event.user_id] = stats

        # Обновляем статистику по временному окну
        window = windowed_stats[event.user_id].now()
        if window is None:
            window = UserStats()
        window.total_events += 1
        window.last_event_time = event.timestamp
        window.total_value += event.value
        windowed_stats[event.user_id] = window


@app.page("/stats/{user_id}")
async def get_user_stats(web, request, user_id: str) -> dict[str, object]:  # noqa: ANN001
    try:
        stats = user_stats_table[user_id]
        if stats is None:
            return web.json(
                {
                    "user_id": user_id,
                    "total_events": 0,
                    "last_event_time": datetime.now(),
                    "total_value": 0.0,
                },
            )
        return web.json(
            {
                "user_id": user_id,
                "total_events": stats.total_events,
                "last_event_time": stats.last_event_time,
                "total_value": stats.total_value,
            },
        )
    except Exception as e:
        return web.json(
            {
                "error": str(e),
                "user_id": user_id,
            },
            status=500,
        )


@app.page("/windowed_stats/{user_id}")
async def get_windowed_stats(web, request, user_id: str) -> dict[str, object]:  # noqa: ANN001
    try:
        window = windowed_stats[user_id].now()
        if window is None:
            return web.json(
                {
                    "user_id": user_id,
                    "total_events": 0,
                    "last_event_time": datetime.now(),
                    "total_value": 0.0,
                },
            )

        return web.json(
            {
                "user_id": user_id,
                "total_events": window.total_events,
                "last_event_time": window.last_event_time,
                "total_value": window.total_value,
            },
        )
    except Exception as e:
        return web.json(
            {
                "error": str(e),
                "user_id": user_id,
            },
            status=500,
        )


if __name__ == "__main__":
    app.main()
