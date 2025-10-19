import logging
import os

from dotenv import load_dotenv


load_dotenv(dotenv_path=".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:9094,localhost:9095,localhost:9096"

INPUT_PRODUCTS_TOPIC = "input-products"

PRODUCER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "ssl.ca.location": "ca.crt",
    "sasl.mechanism": "PLAIN",
    "sasl.username": f"{os.getenv('KAFKA_USERNAME_PRODUCER')}",
    "sasl.password": f"{os.getenv('KAFKA_PASSWORD_PRODUCER')}",
    "acks": "all",
    "enable.idempotence": True,
    "max.in.flight.requests.per.connection": 3,
    "retries": 3,
}

SCHEMA_REGISTRY_CONFIG = {"url": "http://localhost:8081"}
PRODUCT_SCHEMA_STR = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Product",
  "type": "object",
  "properties": {
    "product_id": { "type": "string" },
    "name": { "type": "string" },
    "description": { "type": "string" },
    "price": {
      "type": "object",
      "properties": {
        "amount": { "type": "number" },
        "currency": { "type": "string" }
      },
      "required": ["amount", "currency"]
    },
    "category": { "type": "string" },
    "brand": { "type": "string" },
    "stock": {
      "type": "object",
      "properties": {
        "available": { "type": "integer" },
        "reserved": { "type": "integer" }
      },
      "required": ["available", "reserved"]
    },
    "sku": { "type": "string" },
    "tags": {
      "type": "array",
      "items": { "type": "string" }
    },
    "images": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "url": { "type": "string", "format": "uri" },
          "alt": { "type": "string" }
        },
        "required": ["url", "alt"]
      }
    },
    "specifications": {
      "type": "object",
      "properties": {
        "weight": { "type": "string" },
        "dimensions": { "type": "string" },
        "battery_life": { "type": "string" },
        "water_resistance": { "type": "string" }
      },
      "required": ["weight", "dimensions", "battery_life", "water_resistance"]
    },
    "created_at": { "type": "string", "format": "date-time" },
    "updated_at": { "type": "string", "format": "date-time" },
    "index": { "type": "string" },
    "store_id": { "type": "string" }
  },
  "required": [
    "product_id",
    "name",
    "description",
    "price",
    "category",
    "brand",
    "stock",
    "sku",
    "tags",
    "images",
    "specifications",
    "created_at",
    "updated_at",
    "index",
    "store_id"
  ]
}
"""
