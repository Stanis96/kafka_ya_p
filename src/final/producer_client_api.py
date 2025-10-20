from dataclasses import asdict

import psycopg2

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError, StringSerializer

from src.final.base import (
    POSTGRES_CONFIG,
    PRODUCER_CONFIG,
    SCHEMA_REGISTRY_CONFIG,
    USER_REQUESTS_TOPIC,
    USER_RESPONSES_TOPIC,
    ClientAPICommands,
    UserRequestEvent,
    UserResponseEvent,
    logger,
)
from src.final.schemas import USER_REQUEST_SCHEMA_STR, USER_RESPONSE_SCHEMA_STR


class KafkaClientAPIProducer:
    """
    CLI-приложение клиента: поиск и рекомендации.
    """

    def __init__(self) -> None:
        self.producer = Producer(PRODUCER_CONFIG)
        self.schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)
        self.key_serializer = StringSerializer("utf_8")
        self.request_serializer = JSONSerializer(USER_REQUEST_SCHEMA_STR, self.schema_registry_client)
        self.response_serializer = JSONSerializer(USER_RESPONSE_SCHEMA_STR, self.schema_registry_client)

        self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
        self.pg_cursor = self.pg_conn.cursor()

    @staticmethod
    def _delivery_report(err: KafkaError, msg: Message) -> None:
        msg_val = msg.value().decode() if msg.value() else "None"
        if err:
            logger.error(f"Message '{msg_val}' delivery failed. Error: {err}, Topic: {msg.topic()}")
        else:
            logger.info(
                f"Message '{msg_val}' delivered to {msg.topic()} [Partition {msg.partition()}] at offset {msg.offset()}",
            )

    def send_to_kafka(self, topic: str, key: str, value: dict[str, object], serializer: JSONSerializer) -> None:
        try:
            self.producer.produce(
                topic=topic,
                key=self.key_serializer(key, SerializationContext(topic, MessageField.KEY)),
                value=serializer(value, SerializationContext(topic, MessageField.VALUE)),
                on_delivery=self._delivery_report,
            )
            self.producer.flush()
        except SerializationError as e:
            logger.error(f"Serialization failed for message {value}: {e}")

    def search_product(self, name: str) -> list[dict[str, object]]:
        self.pg_cursor.execute(
            "SELECT product_id, name, price_amount, brand FROM products WHERE name ILIKE %s LIMIT 5;",
            (f"%{name}%",),
        )
        rows = self.pg_cursor.fetchall()

        return [{"product_id": r[0], "name": r[1], "price": float(r[2]), "brand": r[3]} for r in rows]

    def recommend_products(self) -> list[dict[str, object]]:
        self.pg_cursor.execute(
            "SELECT product_id, name, price_amount, stock_available FROM products ORDER BY stock_available DESC LIMIT 3;",
        )
        rows = self.pg_cursor.fetchall()

        return [{"product_id": r[0], "name": r[1], "price": float(r[2]), "stock_available": r[3]} for r in rows]

    def handle_command(self, command: str) -> None:
        if command == ClientAPICommands.SEARCH:
            query = command.split(" ", 1)[1]
            request_event = UserRequestEvent(command=ClientAPICommands.SEARCH, query=query)

            self.send_to_kafka(
                USER_REQUESTS_TOPIC,
                key=ClientAPICommands.SEARCH,
                value=asdict(request_event),
                serializer=self.request_serializer,
            )

            results = self.search_product(query)
            response_event = UserResponseEvent(
                user_id=request_event.user_id,
                command=ClientAPICommands.SEARCH,
                query=query,
                results=results,
            )
            self.send_to_kafka(
                USER_RESPONSES_TOPIC,
                key=ClientAPICommands.SEARCH,
                value=asdict(response_event),
                serializer=self.response_serializer,
            )
            logger.info(
                f"Уважаемый Пользователь {request_event.user_id}!\nПо Вашему запросу найдены товары:\n{results}"
            )

        elif command == ClientAPICommands.RECOMMENDATIONS:
            request_event = UserRequestEvent(command=ClientAPICommands.RECOMMENDATIONS)
            self.send_to_kafka(
                USER_REQUESTS_TOPIC,
                key=ClientAPICommands.RECOMMENDATIONS,
                value=asdict(request_event),
                serializer=self.request_serializer,
            )

            results = self.recommend_products()
            response_event = UserResponseEvent(
                user_id=request_event.user_id,
                command=ClientAPICommands.RECOMMENDATIONS,
                results=results,
            )
            self.send_to_kafka(
                USER_RESPONSES_TOPIC,
                key=ClientAPICommands.RECOMMENDATIONS,
                value=asdict(response_event),
                serializer=self.response_serializer,
            )
            logger.info(
                f"Уважаемый Пользователь {request_event.user_id}!\nПо Вашему запросу найдены рекомендации:\n{results}"
            )

        else:
            logger.warning(f"Неизвестная команда. Используйте: {[x.name for x in ClientAPICommands]}")


if __name__ == "__main__":
    client_api = KafkaClientAPIProducer()
    logger.info(
        f"Доступные команды: {[x.name + '<название товара>' if x.name == ClientAPICommands.SEARCH else x.name for x in ClientAPICommands]}",
    )

    try:
        while True:
            cmd = input("> ").strip()
            if cmd == ClientAPICommands.EXIT:
                break
            client_api.handle_command(cmd)
    except Exception as e:
        logger.error(f"Producer stopped due to error: {e}")
