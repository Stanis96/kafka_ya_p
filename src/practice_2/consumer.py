import time

from confluent_kafka import Consumer, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

from src.practice_2.base import DLQ_TOPIC, PRODUCER_KAFKA_CONFIG, logger


class KafkaOrderConsumer:
    """
    Consumer Kafka для универсального получения сообщений с ручным коммитом оффсета, retry-логикой и DLQ-топиком.

    :param topic: Kafka-топик для сообщений.
    :param kafka_config: Конфигурация консьюмер Kafka.
    :param schema_registry_config: Конфигурация Schema Registry.
    :param json_schema: JSON-схема.
    :param batch_size: Если 1 — читаем по одному сообщению, иначе —
        пачкой.
    :param max_retries: Сколько раз пробовать обработать сообщение перед
        DLQ.
    :param backoff: базовая задержка (в секундах) между ретраями.
    :param dlq_topic: Топик для DLQ.

    """

    def __init__(
        self,
        topic: str,
        kafka_config: dict[str, object],
        schema_registry_config: dict[str, object],
        json_schema: str,
        batch_size: int = 1,
        max_retries: int = 3,
        backoff: float = 1.0,
        dlq_topic: str = DLQ_TOPIC,
    ) -> None:
        self.topic = topic
        self.dlq_topic = dlq_topic
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
        # DLQ producer (можно переиспользовать тот же kafka_config)
        self.dlq_producer = Producer(PRODUCER_KAFKA_CONFIG)
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.backoff = backoff

    @staticmethod
    def _dict_from_dict(data: dict[str, object], ctx: SerializationContext) -> dict[str, object]:
        return data

    @staticmethod
    def _process_message(order: dict[str, object]) -> None:
        """
        Логика обработки сообщения.
        """
        logger.info(f"Processing order: {order}")

    def _process_with_retry(self, order: dict[str, object], raw_msg: Message) -> None:
        """
        Обработка с ретраями.

        Если все попытки неуспешны → DLQ.

        """
        for attempt in range(1, self.max_retries + 1):
            try:
                self._process_message(order)
                return
            except Exception as e:
                logger.warning(f"Processing failed (attempt {attempt}/{self.max_retries}): {e}")
                if attempt < self.max_retries:
                    time.sleep(self.backoff * attempt)
                else:
                    # после всех ретраев → DLQ
                    self._send_to_dlq(raw_msg, reason=str(e))

    def _send_to_dlq(self, msg: Message, reason: str) -> None:
        """
        Отправка сообщения в Dead Letter Queue.
        """
        try:
            dlq_topic = f"{self.topic}.dlq"
            self.dlq_producer.produce(
                topic=dlq_topic,
                key=msg.key(),
                value=msg.value(),
                headers={"error_reason": reason.encode()},
            )
            self.dlq_producer.flush()
            logger.error(f"Sent message to DLQ (offset={msg.offset()}): {reason}")
        except Exception as e:
            logger.exception(f"Failed to send message to DLQ: {e}")

    def __call__(self, poll_timeout: float = 1.0) -> None:  # noqa: PLR0912
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
                            self._process_with_retry(order, msg)
                        else:
                            self._send_to_dlq(msg, reason="Deserialized order is None")
                    except SerializationError:
                        logger.exception(
                            f"Failed to deserialize message: {msg.value()} (topic={msg.topic()}, offset={msg.offset()})",
                        )
                        self._send_to_dlq(msg, reason="SerializationError")

                # Коммитим один раз после обработки пачки
                try:
                    self.consumer.commit(asynchronous=False)
                    logger.info(f"Committed offset for {len(messages)} messages")
                except Exception as e:
                    logger.exception(f"Failed to commit offsets: {e}")
        finally:
            self.consumer.close()
