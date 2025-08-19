import os
import secrets
import time
import uuid

from datetime import datetime

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError, StringSerializer

from src.practice_2.base import JSON_SCHEMA, PRODUCER_KAFKA_CONFIG, SCHEMA_REGISTRY_CONFIG, TOPIC, logger


class KafkaOrderProducer:
    """
    Producer Kafka для отправки сообщений с JSON-схемой через Schema Registry.

    Экземпляр класса можно вызвать как функцию для отправки сообщений.
    Генерирует случайные заказы и отправляет их в указанный топик.

    :param topic: Kafka-топик для сообщений.
    :param kafka_config: Конфигурация продюсера Kafka.
    :param schema_registry_config: Конфигурация Schema Registry.
    :param json_schema: JSON-схема.

    """

    def __init__(
        self,
        topic: str,
        kafka_config: dict[str, object],
        schema_registry_config: dict[str, object],
        json_schema: str,
    ) -> None:
        self.topic = topic
        # Инициализация продюсера
        self.producer = Producer(kafka_config)
        # Инициализация клиента Schema Registry
        self.schema_registry_client = SchemaRegistryClient(schema_registry_config)
        # Создание JSON-сериализатора
        self.json_serializer = JSONSerializer(json_schema, self.schema_registry_client)
        self.key_serializer = StringSerializer("utf_8")

    @staticmethod
    def _generate_random_orders(msg_count: int, is_error_msg: bool = True) -> list[dict[str, object]]:
        """
        Генерация случайных заказов.

        :param msg_count: Количество заказов
        :param is_error_msg: Флаг, добавления первого сообщения с
            ошибкой.
        :return: Список заказов

        """
        orders = []
        for i in range(msg_count):
            order = {
                "order_id": str(uuid.uuid4()),
                "amount": round(100 * secrets.randbelow(10000) / 100, 2),
                "currency": secrets.choice(["USD", "EUR", "RUB"]),
                "created_at": datetime.now().isoformat(),
            }
            if is_error_msg and i == 0:
                order["amount"] = None
            orders.append(order)
        return orders

    @staticmethod
    # Функция обратного вызова для подтверждения доставки
    def _delivery_report(err: KafkaError, msg: Message) -> None:
        msg_val = msg.value().decode() if msg.value() else "None"
        if err:
            logger.error(f"Message '{msg_val}' delivery failed. Error: {err}, Topic: {msg.topic()}")
        else:
            logger.info(
                f"Message '{msg_val}' delivered to {msg.topic()} [Partition {msg.partition()}] at offset {msg.offset()}",
            )

    def __call__(self, msg_count: int, is_error_msg: bool = True) -> None:
        orders = self._generate_random_orders(msg_count, is_error_msg)
        for order in orders:
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=self.key_serializer(
                        order["order_id"],
                        SerializationContext(self.topic, MessageField.KEY),
                    ),
                    value=self.json_serializer(
                        order,
                        SerializationContext(self.topic, MessageField.VALUE),
                    ),
                    on_delivery=self._delivery_report,
                )
            except SerializationError as e:
                logger.error(f"Serialization failed for message: {order}. Error: {e}")
        # Очистка очереди сообщений
        self.producer.flush()


if __name__ == "__main__":
    producer = KafkaOrderProducer(
        topic=TOPIC,
        kafka_config=PRODUCER_KAFKA_CONFIG,
        schema_registry_config=SCHEMA_REGISTRY_CONFIG,
        json_schema=JSON_SCHEMA,
    )

    interval_seconds = int(os.environ.get("SEND_MSG_INTERVAL_SECONDS", 10))
    msg_count = int(os.environ.get("SEND_MSG_COUNT", 10))

    try:
        while True:
            producer(msg_count=msg_count)
            time.sleep(interval_seconds)
    except Exception as e:
        logger.error(f"Producer stopped due to error: {e}")
