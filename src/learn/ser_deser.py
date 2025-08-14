from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer


# Конфигурация для Kafka и Schema Registry
kafka_config = {
    "bootstrap.servers": "localhost:9094",
}


schema_registry_config = {
    "url": "http://localhost:8081",
}


# Определение JSON-схемы
json_schema_str = """
{
 "$schema": "http://json-schema.org/draft-07/schema#",
 "title": "Product",
 "type": "object",
 "properties": {
   "id": {
     "type": "integer"
   },
   "name": {
     "type": "string"
   }
 },
 "required": ["id", "name"]
}
"""


# Инициализация клиента Schema Registry
schema_registry_client = SchemaRegistryClient(schema_registry_config)


# Создание JSON-сериализатора
json_serializer = JSONSerializer(json_schema_str, schema_registry_client)


# Инициализация продюсера
producer = Producer(kafka_config)


# Тема Kafka
topic = "my-topic5"


# Сообщение для отправки
message_value = {"id": 30, "name": "product"}


# Сериализация ключа и значения
key_serializer = StringSerializer("utf_8")
value_serializer = json_serializer


# Функция обратного вызова для подтверждения доставки
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# Отправка сообщения
producer.produce(
    topic=topic,
    key=key_serializer("user_key", SerializationContext(topic, MessageField.VALUE)),
    value=value_serializer(message_value, SerializationContext(topic, MessageField.VALUE)),
    on_delivery=delivery_report,
)


# Очистка очереди сообщений
producer.flush()
