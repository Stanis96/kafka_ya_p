import os

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv


# Загружаем переменные из .env
load_dotenv()


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


if __name__ == "__main__":
    # === Конфигурация подключения к Kafka (Yandex Managed Kafka) ===
    producer_conf = {
        "bootstrap.servers": (
            "rc1a-vp8opmk00g2ngefn.mdb.yandexcloud.net:9091,"
            "rc1b-2pbsl2kjgiprdbku.mdb.yandexcloud.net:9091,"
            "rc1d-cpp99h0ohs0ec7ki.mdb.yandexcloud.net:9091"
        ),
        "security.protocol": "SASL_SSL",
        "ssl.ca.location": "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": f"{os.getenv('KAFKA_USERNAME_PRODUCER')}",
        "sasl.password": f"{os.getenv('KAFKA_PASSWORD_PRODUCER')}",
        "acks": 1,
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 3,
        "retries": 3,
    }

    producer = Producer(producer_conf)

    # === Подключение к Schema Registry (Yandex Cloud) ===
    schema_registry_conf = {
        "url": "https://srnvgadnc5dd3vhrkr30.schema-registry.yandexcloud.net:443",
        "basic.auth.user.info": f"api-key:{os.getenv('SCHEMA_REGISTRY_API_KEY')}",
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # === Получаем схему из реестра ===
    subject = "user-favorites"
    latest = schema_registry_client.get_latest_version(subject)
    schema = latest.schema
    print(f"✅ Используется схема (id={latest.schema_id}) для subject={subject}")

    # === Сериализатор с использованием схемы из реестра ===
    json_serializer = JSONSerializer(schema.schema_str, schema_registry_client, user_to_dict)

    topic = "test-topic"  # Название топика в Kafka
    user = {
        "name": "Cloud user",
        "favoriteNumber": 777,
        "favoriteColor": "green",
    }

    context = SerializationContext(topic, MessageField.VALUE)
    serialized_value = json_serializer(user, context)

    # === Отправляем сообщение ===
    producer.produce(
        topic=topic,
        key="cloud-user",
        value=serialized_value,
        headers=[("source", b"cloud-kafka-producer")],
        callback=delivery_report,
    )
    producer.flush()
