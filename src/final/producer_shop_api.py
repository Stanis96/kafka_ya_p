import json

from pathlib import Path

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError, StringSerializer

from src.final.base import (
    INPUT_PRODUCTS_TOPIC,
    PRODUCER_CONFIG,
    SCHEMA_REGISTRY_CONFIG,
    logger,
)
from src.final.schemas import PRODUCT_SCHEMA_STR


class KafkaProductProducer:
    """
    Producer Kafka для отправки сообщений с JSON-схемой через Schema Registry.

    Экземпляр класса можно вызвать как функцию для отправки сообщений.

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
        self.producer = Producer(kafka_config)
        self.schema_registry_client = SchemaRegistryClient(schema_registry_config)
        self.json_serializer = JSONSerializer(json_schema, self.schema_registry_client)
        self.key_serializer = StringSerializer("utf_8")

    @staticmethod
    def _get_products(products_json_path: Path) -> list[dict[str, object]]:
        """
        Получение списка продуктов из JSON-файла.

        :param products_json_path: Путь к JSON-файлу с продуктами.
        :return: Список словарей с продуктами.

        """

        if not products_json_path.exists():
            logger.error(f"Файл {products_json_path} не найден.")
            return []

        try:
            with products_json_path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            if not isinstance(data, dict) or "products" not in data:
                logger.error("Некорректный формат JSON-файла: ожидалось поле 'products'.")
                return []

            products = data["products"]
            if not isinstance(products, list):
                logger.error("Некорректный тип поля 'products': ожидался список.")
                return []

            logger.info(f"Загружено {len(products)} продуктов из файла {products_json_path}.")
            return products

        except json.JSONDecodeError as e:
            logger.error(f"Ошибка разбора JSON-файла {products_json_path}: {e}")
        except Exception as e:
            logger.error(f"Не удалось прочитать {products_json_path}: {e}")

        return []

    @staticmethod
    def _delivery_report(err: KafkaError, msg: Message, product_id: str) -> None:
        if err:
            logger.error(f"Message for product {product_id} delivery failed: {err}")
        else:
            logger.info(
                f"Message for product {product_id} delivered to {msg.topic()} "
                f"[Partition {msg.partition()}] at offset {msg.offset()}",
            )

    def __call__(self, products_json_path: Path) -> None:
        products = self._get_products(products_json_path=products_json_path)
        for product in products:
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=self.key_serializer(
                        product["product_id"],
                        SerializationContext(self.topic, MessageField.KEY),
                    ),
                    value=self.json_serializer(
                        product,
                        SerializationContext(self.topic, MessageField.VALUE),
                    ),
                    on_delivery=lambda err, msg, pid=product["product_id"]: self._delivery_report(err, msg, pid),
                )
            except SerializationError as e:
                logger.error(f"Serialization failed for message: {product}. Error: {e}")
        self.producer.flush()


if __name__ == "__main__":
    producer = KafkaProductProducer(
        topic=INPUT_PRODUCTS_TOPIC,
        kafka_config=PRODUCER_CONFIG,
        schema_registry_config=SCHEMA_REGISTRY_CONFIG,
        json_schema=PRODUCT_SCHEMA_STR,
    )

    products_json_path = Path(__file__).parent / "data_source" / "products.json"

    try:
        producer(products_json_path=products_json_path)
    except Exception as e:
        logger.error(f"Producer stopped due to error: {e}")
