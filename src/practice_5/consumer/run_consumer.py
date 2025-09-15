import json
import logging

from confluent_kafka import Consumer


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

consumer_config = {
    "bootstrap.servers": "kafka-0:9092,kafka-1:9092,kafka-2:9092",
    "group.id": "consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

topics = ["^customers\\.public\\..*"]

consumer = Consumer(consumer_config)
consumer.subscribe(topics)


def main() -> None:
    logger.info(f"Listening to Kafka topics: {topics}")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if not msg:
                continue

            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else None
            value = msg.value().decode("utf-8") if msg.value() else None

            try:
                value_json = json.loads(value)
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}, value={value}")
                value_json = value
            except Exception as e:
                logger.exception(f"Unexpected consumer error: {e}")
                value_json = msg.value()

            msg_offset = msg.offset()
            payload = value_json.get("payload") if isinstance(value_json, dict) else value_json
            logger.info(
                f"Topic: {msg.topic()}, Offset: {msg_offset}, Key: {key}, Payload: {json.dumps(payload, indent=2)}\n",
            )

            try:
                consumer.commit(asynchronous=False)
                logger.info(f"Committed offset {msg_offset} message")
            except Exception as e:
                logger.exception(f"Failed to commit offset: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
