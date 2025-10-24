import json
import ssl

import asyncpg

from faust import App, Record, SASLCredentials, Stream
from faust.serializers.codecs import Codec

from src.final.base import (
    BOOTSTRAP_SERVERS,
    CA_CERT,
    FAUST_APP_NAME,
    INPUT_PRODUCTS_TOPIC,
    KAFKA_FAUST_PASSWORD,
    KAFKA_FAUST_USERNAME,
    POSTGRES_DSN,
    PRODUCTS_TOPIC,
    logger,
)


app = App(
    FAUST_APP_NAME,
    broker=f"kafka://{BOOTSTRAP_SERVERS}",
    store="memory://",
    broker_credentials=SASLCredentials(
        username=KAFKA_FAUST_USERNAME,
        password=KAFKA_FAUST_PASSWORD,
        ssl_context=ssl.create_default_context(cafile=CA_CERT),
    ),
    consumer_auto_offset_reset="earliest",
)


class Price(Record, serializer="json"):  # noqa: D101
    amount: float
    currency: str


class Stock(Record, serializer="json"):  # noqa: D101
    available: int
    reserved: int


class Image(Record, serializer="json"):  # noqa: D101
    url: str
    alt: str


class Specifications(Record, serializer="json"):  # noqa: D101
    weight: str
    dimensions: str
    battery_life: str
    water_resistance: str


class Product(Record, serializer="json"):  # noqa: D101
    product_id: str
    name: str
    description: str
    price: Price
    category: str
    brand: str
    stock: Stock
    sku: str
    tags: list[str]
    images: list[Image]
    specifications: Specifications
    created_at: str
    updated_at: str
    index: str
    store_id: str


class ConfluentJSONCodec(Codec):
    """
    Декодер/кодер для сообщений в формате Confluent JSON Schema.
    """

    def loads(self, s: bytes) -> Product | None:
        if not s:
            return None
        try:
            return json.loads(s[5:].decode("utf-8"))
        except Exception as e:
            logger.error(f"Ошибка декодирования Confluent JSON: {e}, raw={s[:20]!r}")
            raise

    def dumps(self, obj: Product) -> bytes:
        try:
            return json.dumps(obj).encode("utf-8")
        except Exception as e:
            logger.error(f"Ошибка кодирования Confluent JSON: {e}, obj={obj}")
            return b"{}"


confluent_json_codec = ConfluentJSONCodec()

input_topic = app.topic(INPUT_PRODUCTS_TOPIC, value_type=Product, key_type=str, value_serializer=confluent_json_codec)
output_topic = app.topic(PRODUCTS_TOPIC, value_type=Product, key_type=str)


@app.agent(input_topic)
async def process_products(stream: Stream[Product]) -> None:
    pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=1, max_size=5)
    logger.info("Connected to PostgreSQL")

    async for product in stream:
        logger.warning(f"Product {product}")

        async with pool.acquire() as conn:
            forbidden = await conn.fetchval(
                "SELECT 1 FROM forbidden_products WHERE product_id = $1",
                product.product_id,
            )

        if forbidden:
            logger.warning(f"❌ Product {product.product_id} is forbidden, skipping.")
        else:
            await output_topic.send(value=product, key=product.product_id)
            logger.info(f"✅ Product {product.product_id} sent to {PRODUCTS_TOPIC}.")


@app.task
async def on_startup() -> None:
    logger.info(f"Faust {FAUST_APP_NAME} is starting...")


if __name__ == "__main__":
    app.main()
