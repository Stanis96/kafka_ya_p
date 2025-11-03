import json
import logging
import os
import sys

from datetime import datetime
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv
from hdfs import InsecureClient
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType


# Все базовые настройки (т.к base.py несовместим с 3.8)
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS_CLUSTER2 = "localhost:4094,localhost:5094,localhost:6094"

HDFS_CLIENT_URL = "http://localhost:9870"
HDFS_USER = "root"
USER_REQUESTS_TOPIC = "user-requests"
USER_RESPONSES_TOPIC = "user-responses"
ANALYSIS_TOPIC = "analysis"

SPARK_MASTER_URL = "spark://localhost:7077"

PRODUCER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVERS_CLUSTER2,
    "security.protocol": "SASL_SSL",
    "ssl.ca.location": str(BASE_DIR / "ca.crt"),
    "sasl.mechanism": "PLAIN",
    "sasl.username": os.getenv("KAFKA_USERNAME_PRODUCER"),
    "sasl.password": os.getenv("KAFKA_PASSWORD_PRODUCER"),
    "acks": "all",
    "enable.idempotence": True,
    "max.in.flight.requests.per.connection": 3,
    "retries": 3,
}

request_schema = StructType(
    [
        StructField("command", StringType(), True),
        StructField("query", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
    ],
)

response_schema = StructType(
    [
        StructField("command", StringType(), True),
        StructField("query", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField(
            "results",
            ArrayType(
                StructType(
                    [
                        StructField("product_id", StringType(), True),
                        StructField("name", StringType(), True),
                        StructField("price", DoubleType(), True),
                        StructField("brand", StringType(), True),
                    ],
                ),
            ),
            True,
        ),
    ],
)


def read_all_clean(client: InsecureClient, topic: str): #noqa: ANN201
    """
    Чтение всех доступных файлов из HDFS по топику, по всем дням и часам.

    Печатает только найденные валидные JSON.

    """
    all_jsons = []
    try:
        days = client.list(f"/data/kafka/{topic}/")
        for day_folder in days:
            if not day_folder.startswith("dt="):
                continue
            day_path = f"/data/kafka/{topic}/{day_folder}"
            for h in range(24):
                hour_path = f"{day_path}/hour={h}"
                try:
                    files = client.list(hour_path)
                    for f_name in files:
                        full_path = f"{hour_path}/{f_name}"
                        with client.read(full_path, encoding="utf-8") as reader:
                            for line in reader:
                                try:
                                    j = json.loads(line)
                                    value = j.get("value")
                                    if not value:
                                        continue
                                    idx = value.find("{")
                                    if idx != -1:
                                        clean_json = value[idx:]
                                        # проверяем, валиден ли JSON
                                        json.loads(clean_json)
                                        all_jsons.append(clean_json)
                                        logger.info(f"Найден JSON: {clean_json[:100]}...")
                                except json.JSONDecodeError:
                                    logger.warning(f"Невалидный JSON в {full_path}: {line[:50]}...")
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"Ошибка при листинге /data/kafka/{topic}/: {e}")

    logger.info(f"Собрано {len(all_jsons)} валидных JSON из {topic}")
    return all_jsons


if __name__ == "__main__":
    spark = SparkSession.builder.appName("UserAnalyticsHDFS").master(SPARK_MASTER_URL).getOrCreate()
    hdfs = InsecureClient(HDFS_CLIENT_URL, user=HDFS_USER)

    requests_lines = read_all_clean(hdfs, USER_REQUESTS_TOPIC)
    responses_lines = read_all_clean(hdfs, USER_RESPONSES_TOPIC)

    if not requests_lines and not responses_lines:
        logger.info("Данных нет, завершаем")
        sys.exit()

    requests_df = spark.read.json(spark.sparkContext.parallelize(requests_lines, 1), schema=request_schema)
    responses_df = spark.read.json(spark.sparkContext.parallelize(responses_lines, 1), schema=response_schema)

    # Добавляем results_count
    responses_df = responses_df.withColumn(
        "results_count",
        f.size(f.coalesce(f.col("results"), f.array())),
    )

    # Подсчёт аналитики
    requests_count = requests_df.count()
    responses_count = responses_df.count()
    avg_results_count = responses_df.agg(f.avg("results_count")).collect()[0][0] or 0.0

    # Вывод
    responses_df.show(truncate=False)

    analytics_result = {
        "timestamp": datetime.now().isoformat(),
        "requests_count": requests_count,
        "responses_count": responses_count,
        "avg_results_count": avg_results_count,
    }

    logger.info(f"Analytics: {analytics_result}")

    # ===== Отправка в Kafka =====
    producer = Producer(PRODUCER_CONFIG)
    key_bytes = analytics_result["timestamp"].encode("utf-8")
    value_bytes = json.dumps(analytics_result).encode("utf-8")

    producer.produce(
        topic=ANALYSIS_TOPIC,
        key=key_bytes,
        value=value_bytes,
        callback=lambda err, msg: logger.info(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}") if err is None else logger.error(f"Delivery failed: {err}"),
    )
    producer.flush()

