import os

from src.practice_2.base import (
    CONSUMER_BATCH_KAFKA_CONFIG,
    CONSUMER_SINGLE_MSG_KAFKA_CONFIG,
    JSON_SCHEMA,
    SCHEMA_REGISTRY_CONFIG,
    TOPIC,
    logger,
)
from src.practice_2.consumer import KafkaOrderConsumer


if __name__ == "__main__":
    batch_size = int(os.environ.get("BATCH_SIZE", 1))
    if batch_size > 1:
        kafka_config = CONSUMER_BATCH_KAFKA_CONFIG
        logger.info(f"Running in batch mode. Batch size: {batch_size}")
    else:
        kafka_config = CONSUMER_SINGLE_MSG_KAFKA_CONFIG
        logger.info("Running in single message mode.")

    consumer = KafkaOrderConsumer(
        topic=TOPIC,
        kafka_config=kafka_config,
        schema_registry_config=SCHEMA_REGISTRY_CONFIG,
        json_schema=JSON_SCHEMA,
        batch_size=batch_size,
    )

    consumer(poll_timeout=1.0)
