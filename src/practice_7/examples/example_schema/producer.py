from confluent_kafka import Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext


def user_to_dict(user, ctx):
    """
    Сериализация объекта пользователя в dict.
    """
    return user


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered message to topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


USER_SCHEMA_STR = """
{
 "$schema": "http://json-schema.org/draft-07/schema#",
 "title": "User",
 "type": "object",
 "properties": {
    "name": { "type": "string" },
    "favoriteNumber": { "type": "integer" },
    "favoriteColor": { "type": "string" }
 },
 "required": ["name", "favoriteNumber", "favoriteColor"]
}
"""


if __name__ == "__main__":
    bootstrap_servers = "localhost:9094,localhost:9095,localhost:9096"
    schema_registry_url = "http://localhost:8081"
    topic = "user"
    subject = topic + "-value"

    producer_conf = {
        "bootstrap.servers": bootstrap_servers,
    }
    producer = Producer(producer_conf)
    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url, })

    try:
        latest = schema_registry_client.get_latest_version(subject)
        print(f"Schema is already registered for {subject}:\n{latest.schema.schema_str}")
    except Exception:
        schema_object = Schema(USER_SCHEMA_STR, "JSON")
        schema_id = schema_registry_client.register_schema(subject, schema_object)
        print(f"Registered schema for {subject} with id: {schema_id}")

    json_serializer = JSONSerializer(USER_SCHEMA_STR, schema_registry_client, user_to_dict)
    user = {
        "name": "First user",
        "favoriteNumber": 42,
        "favoriteColor": "blue",
    }
    context = SerializationContext(topic, MessageField.VALUE)
    serialized_value = json_serializer(user, context)

    producer.produce(
        topic,
        key="first-user",
        value=serialized_value,
        headers=[("myTestHeader", b"header values are binary")],
        callback=delivery_report,
    )
    producer.flush()
