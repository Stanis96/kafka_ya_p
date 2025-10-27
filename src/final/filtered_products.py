import ssl

import asyncpg
import faust

from schema_registry.client import SchemaRegistryClient
from schema_registry.serializers.faust import FaustJsonSerializer

from src.final.base import (
    BOOTSTRAP_SERVERS,
    CA_CERT,
    FAUST_APP_NAME,
    INPUT_PRODUCTS_TOPIC,
    KAFKA_FAUST_PASSWORD,
    KAFKA_FAUST_USERNAME,
    POSTGRES_DSN,
    PRODUCTS_TOPIC,
    SCHEMA_REGISTRY_CONFIG,
    logger,
)
from src.final.schemas import PRODUCT_SCHEMA_STR


app = faust.App(
    FAUST_APP_NAME,
    broker=f"kafka://{BOOTSTRAP_SERVERS}",
    store="memory://",
    broker_credentials=faust.SASLCredentials(
        username=KAFKA_FAUST_USERNAME,
        password=KAFKA_FAUST_PASSWORD,
        ssl_context=ssl.create_default_context(cafile=CA_CERT),
    ),
    consumer_auto_offset_reset="earliest",
)


client = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)
product_serializer = FaustJsonSerializer(client, INPUT_PRODUCTS_TOPIC, PRODUCT_SCHEMA_STR)


faust.serializers.codecs.codecs["json_products"] = product_serializer
client.register(
    subject=f"{PRODUCTS_TOPIC}-value",
    schema=PRODUCT_SCHEMA_STR,
    schema_type="JSON",
)


class ProductModel(faust.Record, serializer="json_products"):  # noqa: D101
    product_id: str
    name: str
    description: str
    price: dict
    category: str
    brand: str
    stock: dict
    sku: str
    tags: list[str]
    images: list[dict]
    specifications: dict
    created_at: str
    updated_at: str
    index: str
    store_id: str


input_topic = app.topic(INPUT_PRODUCTS_TOPIC, value_type=ProductModel)
output_topic = app.topic(PRODUCTS_TOPIC, value_type=ProductModel)


@app.agent(input_topic)
async def process_products(records: faust.Stream[ProductModel]) -> None:
    pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=1, max_size=5)
    logger.info("Connected to PostgreSQL")

    async for product in records:
        if product is None:
            logger.warning("❌ Received empty message, skipping")
            continue

        async with pool.acquire() as conn:
            forbidden = await conn.fetchval(
                "SELECT 1 FROM forbidden_products WHERE product_id = $1",
                product.product_id,
            )

        if forbidden:
            logger.warning(f"❌ Product {product.product_id} is forbidden, skipping.")
            continue

        await output_topic.send(key=product.product_id, value=product)
        logger.info(f"✅ Product {product.product_id} sent to {PRODUCTS_TOPIC}")
