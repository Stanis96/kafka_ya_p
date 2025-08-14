import logging


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# Тема Kafka
TOPIC = "practice_2"
GROUP_ID_CONSUMER_SINGLE_MSG = "consumer_single_msg"
GROUP_ID_CONSUMER_BATCH_MSG = "consumer_batch_msg"

SCHEMA_REGISTRY_CONFIG = {
    "url": "http://schema-registry:8081",
}

# Определение JSON-схемы
JSON_SCHEMA = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Order",
  "type": "object",
  "properties": {
    "order_id": { "type": "string" },
    "amount":   { "type": "number" },
    "currency": { "type": "string" },
    "created_at": { "type": "string", "format": "date-time" }
  },
  "required": ["order_id", "amount", "currency"]
}
"""

# Конфигурация для Kafka и Schema Registry
PRODUCER_KAFKA_CONFIG = {
    "bootstrap.servers": "kafka-0:9092,kafka-1:9092",
    "acks": "all",  # Подтверждение от всех реплик
    "retries": 5,  # Количество попыток переотправки
    "retry.backoff.ms": 500,  # Задержка между попытками
}

CONSUMER_SINGLE_MSG_KAFKA_CONFIG = {
    "bootstrap.servers": "kafka-0:9092,kafka-1:9092",
    "group.id": GROUP_ID_CONSUMER_SINGLE_MSG,
    "auto.offset.reset": "earliest",  # читать с начала, если оффсеты отсутствуют
    "enable.auto.commit": False,  # ручной коммит
}

CONSUMER_BATCH_KAFKA_CONFIG = {
    "bootstrap.servers": "kafka-0:9092,kafka-1:9092",
    "group.id": GROUP_ID_CONSUMER_BATCH_MSG,
    "auto.offset.reset": "earliest",  # читать с начала, если оффсеты отсутствуют
    "enable.auto.commit": False,  # ручной коммит
    "fetch.min.bytes": 1024,  # минимальный размер данных
    "fetch.wait.max.ms": 500,  # максимальное время ожидания
}
