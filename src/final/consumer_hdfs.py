import json
import uuid

from datetime import datetime

from confluent_kafka import Consumer
from hdfs import InsecureClient

from src.final.base import (
    CONSUMER_CONFIG,
    HDFS_CLIENT_URL,
    HDFS_USER,
    USER_REQUESTS_TOPIC,
    USER_RESPONSES_TOPIC,
    logger,
)


def hdfs_path(topic: str) -> str:
    now = datetime.now()
    return f"/data/kafka/{topic}/dt={now:%Y-%m-%d}/hour={now:%H}"


if __name__ == "__main__":
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe([USER_REQUESTS_TOPIC, USER_RESPONSES_TOPIC])

    hdfs = InsecureClient(HDFS_CLIENT_URL, user=HDFS_USER)
    buffers = {USER_REQUESTS_TOPIC: [], USER_RESPONSES_TOPIC: []}

    try:
        while True:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Ошибка: {msg.error()}")
                continue

            topic = msg.topic()
            value = msg.value().decode("utf-8")

            logger.info(
                f"Получено сообщение: {value=}, partition={msg.partition()}, offset={msg.offset()}",
            )

            record = {
                "value": value,
                "topic": topic,
                "partition": msg.partition(),
                "offset": msg.offset(),
                "timestamp": msg.timestamp()[1],
            }

            buffers[topic].append(json.dumps(record))

            path = hdfs_path(topic)
            hdfs.makedirs(path)
            file = f"{path}/batch_{uuid.uuid4()}.json"

            with hdfs.write(file, encoding="utf-8") as w:
                w.write("\n".join(buffers[topic]))

            logger.info(f"Записан batch {len(buffers[topic])} сообщений в {file}")
            buffers[topic].clear()
    except KeyboardInterrupt:
        logger.info("Программа завершена пользователем")
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")
    finally:
        consumer.close()
