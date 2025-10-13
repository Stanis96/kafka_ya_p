import os

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv


# Загружаем переменные из .env
load_dotenv()


def user_from_dict(user, ctx):
    """
    Десериализация объекта пользователя из dict.
    """
    return user


if __name__ == "__main__":
    topic = "test-topic"

    # === Конфигурация подключения к облачной Kafka ===
    consumer_conf = {
        "bootstrap.servers": (
            "rc1a-vp8opmk00g2ngefn.mdb.yandexcloud.net:9091,"
            "rc1b-2pbsl2kjgiprdbku.mdb.yandexcloud.net:9091,"
            "rc1d-cpp99h0ohs0ec7ki.mdb.yandexcloud.net:9091"
        ),
        "security.protocol": "SASL_SSL",
        "ssl.ca.location": "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": f"{os.getenv('KAFKA_USERNAME_CONSUMER')}",
        "sasl.password": f"{os.getenv('KAFKA_PASSWORD_CONSUMER')}",
        "group.id": "cloud-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000,
        "max.poll.interval.ms": 300000,
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    # === Подключение к Schema Registry ===
    schema_registry_conf = {
        "url": "https://srnvgadnc5dd3vhrkr30.schema-registry.yandexcloud.net:443",
        "basic.auth.user.info": f"api-key:{os.getenv('SCHEMA_REGISTRY_API_KEY')}",
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    latest = schema_registry_client.get_latest_version("user-favorites")
    json_schema_str = latest.schema.schema_str
    json_deserializer = JSONDeserializer(json_schema_str, from_dict=user_from_dict)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            context = SerializationContext(msg.topic(), MessageField.VALUE)
            user = json_deserializer(msg.value(), context)
            print(f"Message on {msg.topic()}:\n{user}")
            consumer.commit(msg)

            if msg.headers():
                print(f"Headers: {msg.headers()}")

    except KeyboardInterrupt:
        print("Consumer interrupted by user")
    finally:
        consumer.close()
        print("Closing consumer")
