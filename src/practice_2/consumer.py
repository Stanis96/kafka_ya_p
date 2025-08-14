from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

from src.practice_2.base import logger


class KafkaOrderConsumer:
    """
    Consumer Kafka для универсального получения сообщений с ручным коммитом оффсета.

    :param topic: Kafka-топик для сообщений.
    :param kafka_config: Конфигурация консьюмер Kafka.
    :param schema_registry_config: Конфигурация Schema Registry.
    :param json_schema: JSON-схема.
    :param batch_size: Если 1 — читаем по одному сообщению, иначе —
        пачкой.

    """

    def __init__(
        self,
        topic: str,
        kafka_config: dict[str, object],
        schema_registry_config: dict[str, object],
        json_schema: str,
        batch_size: int = 1,
    ) -> None:
        self.topic = topic
        # Инициализация консьюмера
        self.consumer = Consumer(kafka_config)
        # Подписка на топик
        self.consumer.subscribe([topic])
        # Инициализация клиента Schema Registry
        self.schema_registry_client = SchemaRegistryClient(schema_registry_config)
        # Создание JSON-десериализатора
        self.json_deserializer = JSONDeserializer(
            schema_str=json_schema,
            from_dict=self._dict_from_dict,
            schema_registry_client=self.schema_registry_client,
        )
        self.batch_size = batch_size

    @staticmethod
    def _dict_from_dict(data: dict[str, object], ctx: SerializationContext) -> dict[str, object]:
        return data

    @staticmethod
    def _process_message(order: dict[str, object]) -> None:
        """
        Логика обработки сообщения.
        """
        logger.info(f"Processing order: {order}")

    def __call__(self, poll_timeout: float = 1.0) -> None:
        try:
            while True:
                if self.batch_size == 1:
                    # Чтение по одному сообщению
                    msg = self.consumer.poll(timeout=poll_timeout)
                    messages = [msg] if msg else []
                else:
                    # Чтение пачкой
                    messages = self.consumer.consume(num_messages=self.batch_size, timeout=poll_timeout)

                if not messages:
                    logger.debug("No messages available within timeout. Continue polling.")
                    continue

                for msg in messages:
                    if msg is None:
                        continue

                    if msg.error():
                        logger.error(f"Consumer error: {msg.error()}")
                        continue

                    try:
                        order = self.json_deserializer(
                            msg.value(),
                            SerializationContext(self.topic, MessageField.VALUE),
                        )
                        if order is not None:
                            self._process_message(order)
                    except SerializationError:
                        logger.exception(
                            f"Failed to deserialize message: {msg.value()} (topic={msg.topic()}, offset={msg.offset()})",
                        )

                # Коммитим один раз после обработки пачки
                try:
                    self.consumer.commit(asynchronous=False)
                    logger.info(f"Committed offset for {len(messages)} messages")
                except Exception as e:
                    logger.exception(f"Failed to commit offsets: {e}")
        finally:
            self.consumer.close()
