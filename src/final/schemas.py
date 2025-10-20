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

USER_REQUEST_SCHEMA_STR = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "UserRequest",
  "type": "object",
  "properties": {
    "user_id": {"type": "string"},
    "command": {"type": "string"},
    "query": {"type": ["string", "null"]},
    "timestamp": {"type": "string", "format": "date-time"}
  },
  "required": ["command", "timestamp", "user_id"]
}
"""

USER_RESPONSE_SCHEMA_STR = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "UserResponse",
  "type": "object",
  "properties": {
    "user_id": {"type": "string"},
    "command": {"type": "string"},
    "query": {"type": ["string", "null"]},
    "results": {"type": "array", "items": {"type": "object"}},
    "timestamp": {"type": "string", "format": "date-time"}
  },
  "required": ["command", "results", "user_id", "timestamp"]
}
"""
