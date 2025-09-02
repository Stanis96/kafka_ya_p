from faust import App, Stream
from faust.types import TableT

from src.practice_3.base import (
    BLOCKED_USERS_TOPIC,
    FAUST_APP_ID,
    FILTERED_MESSAGES_TOPIC,
    FORBIDDEN_WORDS_TOPIC,
    KAFKA_BROKER,
    MESSAGES_TOPIC,
    logger,
)
from src.practice_3.models import BlockUserEvent, CensoredMessage, ForbiddenAction, ForbiddenWordEvent, Message


app = App(FAUST_APP_ID, broker=KAFKA_BROKER)

# Tables
blocked_users = app.Table("blocked_users_table", default=list, partitions=4)
forbidden_words = app.Table("forbidden_words_table", default=list, partitions=4)

# Topics
messages_topic = app.topic(MESSAGES_TOPIC, key_type=str, value_type=Message)
blocked_users_topic = app.topic(BLOCKED_USERS_TOPIC, key_type=str, value_type=BlockUserEvent)
filtered_messages_topic = app.topic(FILTERED_MESSAGES_TOPIC, key_type=str, value_type=CensoredMessage)
forbidden_words_topic = app.topic(FORBIDDEN_WORDS_TOPIC, key_type=str, value_type=ForbiddenWordEvent)


def update_set_table(table: TableT, key: str, value: str, action: ForbiddenAction = ForbiddenAction.ADD.value) -> None:
    """
    Универсальное обновление таблицы множества.
    """
    current_set: set[str] = set(table.get(key, []))
    if action == ForbiddenAction.ADD.value:
        current_set.add(value)
    elif action == ForbiddenAction.REMOVE.value:
        current_set.discard(value)
    table[key] = list(current_set)


def censor_text(text: str, forbidden: set[str]) -> str:
    """
    Возвращает текст с заменой запрещённых слов на '*' символы.
    """
    return " ".join("*" * len(w) if w.lower() in forbidden else w for w in text.split())


@app.agent(blocked_users_topic)
async def update_blocked_users(stream: Stream[BlockUserEvent]) -> None:
    async for event in stream:
        update_set_table(blocked_users, event.user, event.blocked)
        logger.info(f"[Blocked Users] User {event.user} blocked {event.blocked}")


@app.agent(forbidden_words_topic)
async def update_forbidden_words(stream: Stream[ForbiddenWordEvent]) -> None:
    async for event in stream:
        word = event.word.lower()
        update_set_table(forbidden_words, "list", word, action=event.action)
        logger.info(f"[Forbidden Words] {event.action}: {event.word}")


@app.agent(messages_topic)
async def process_messages(stream: Stream[Message]) -> None:
    async for msg in stream:
        if msg.sender in set(blocked_users.get(msg.recipient, [])):
            logger.info(f"[Message Blocked] {msg.sender} → {msg.recipient}")
            continue

        censored_msg = CensoredMessage(
            sender=msg.sender,
            recipient=msg.recipient,
            text=censor_text(msg.text, set(forbidden_words.get("list", []))),
        )
        await filtered_messages_topic.send(key=msg.recipient, value=censored_msg)
        logger.info(f"[Message Sent] {censored_msg}")


@app.task(on_leader=True)
async def produce_test_data() -> None:
    """
    Отправка тестовых данных в топики.
    """
    for word in ["жесть", "ужас", "лох"]:
        await forbidden_words_topic.send(value=ForbiddenWordEvent(action=ForbiddenAction.ADD.value, word=word))

    await blocked_users_topic.send(value=BlockUserEvent(user="Алина", blocked="Андрей"))

    messages = [
        Message(sender="Виктор", recipient="Алина", text="Привет, Алина!"),
        Message(sender="Андрей", recipient="Алина", text="Какой ужас ! Ты просто жесть !"),
        Message(sender="Андрей", recipient="Виктор", text="Здарова, Виктор!"),
        Message(sender="Виктор", recipient="Андрей", text="Андрей, вы лох !"),
    ]
    for msg in messages:
        await messages_topic.send(key=msg.recipient, value=msg)


if __name__ == "__main__":
    app.main()
