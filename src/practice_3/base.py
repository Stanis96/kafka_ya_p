import logging

from enum import Enum


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


FAUST_APP_ID = "chat-filter-app"

KAFKA_BROKER = "kafka://kafka-0:9092,kafka-1:9092,kafka-2:9092"

MESSAGES_TOPIC = "messages"
BLOCKED_USERS_TOPIC = "blocked_users"
FILTERED_MESSAGES_TOPIC = "filtered_messages"
FORBIDDEN_WORDS_TOPIC = "forbidden_words"


class ForbiddenAction(Enum):  # noqa: D101
    ADD = "add"
    REMOVE = "remove"
