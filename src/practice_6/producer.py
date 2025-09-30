import logging
import time
import uuid

from confluent_kafka import Producer


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logger.info("Запуск ```producer```...")

    producer_conf = {
        "bootstrap.servers": "kafka-1:1090",
        "security.protocol": "SASL_SSL",
        "ssl.ca.location": "ca.crt",
        "ssl.certificate.location": "kafka-1-creds/kafka-1.crt",
        "ssl.key.location": "kafka-1-creds/kafka-1.key",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "producer",
        "sasl.password": "your_password",
    }

    try:
        producer = Producer(producer_conf)

        while True:
            key = f"key-{uuid.uuid4()}"
            value = "Hey SSL/SASL"
            producer.produce(
                "topic-1",
                key=key,
                value=value,
            )
            producer.flush()
            logger.info(f"Отправлено сообщение: {key=}, {value=}")
            time.sleep(3)
    except Exception as e:
        logger.error(e)
