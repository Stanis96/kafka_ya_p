import uuid

from confluent_kafka import Producer


if __name__ == "__main__":
    config = {
        "bootstrap.servers": "localhost:9093",
        # Настройки SASL-аутентификации
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "admin",
        "sasl.password": "admin-secret",
    }

    producer = Producer(config)

    key = f"key-{uuid.uuid4()}"
    value = "SASL/PLAIN"
    producer.produce(
        "sasl-plain-topic",
        key=key,
        value=value,
    )
    producer.flush()
    print(f"Отправлено сообщение: {key=}, {value=}")
