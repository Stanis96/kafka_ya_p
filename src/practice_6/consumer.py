import logging

from confluent_kafka import Consumer


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logger.info("Запуск ```consumer```...")

    consumer_conf = {
        "bootstrap.servers": "kafka-1:1090",
        "group.id": "consumer",
        "auto.offset.reset": "earliest",
        "security.protocol": "SASL_SSL",
        "ssl.ca.location": "ca.crt",
        "ssl.certificate.location": "kafka-1-creds/kafka-1.crt",
        "ssl.key.location": "kafka-1-creds/kafka-1.key",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "consumer",
        "sasl.password": "your_password",
    }

    try:
        consumer = Consumer(consumer_conf)
        consumer.subscribe(["topic-1", "topic-2"])

        try:
            while True:
                message = consumer.poll(1)

                if message is None:
                    continue
                if message.error():
                    logger.error(f"Ошибка: {message.error()}")
                    continue

                key = message.key().decode("utf-8")
                value = message.value().decode("utf-8")
                offset = message.offset()
                logger.info(f"Получено сообщение: key='{key}', value='{value}', {offset=}")
        finally:
            consumer.close()
    except Exception as e:
        logger.error(e)
