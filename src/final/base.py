import logging
import os
import secrets

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent

load_dotenv(dotenv_path=BASE_DIR / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:1091"
CA_CERT = str(BASE_DIR / "ca.crt")

INPUT_PRODUCTS_TOPIC = "input-products"
USER_REQUESTS_TOPIC = "user-requests"
USER_RESPONSES_TOPIC = "user-responses"
PRODUCTS_TOPIC = "products"

USER_LIST = ["user_001", "user_002", "user_003"]

FAUST_APP_NAME = "product-filter-app"
KAFKA_FAUST_USERNAME = os.getenv("KAFKA_USERNAME_FAUST")
KAFKA_FAUST_PASSWORD = os.getenv("KAFKA_PASSWORD_FAUST")

PRODUCER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "ssl.ca.location": CA_CERT,
    "sasl.mechanism": "PLAIN",
    "sasl.username": os.getenv("KAFKA_USERNAME_PRODUCER"),
    "sasl.password": os.getenv("KAFKA_PASSWORD_PRODUCER"),
    "acks": "all",
    "enable.idempotence": True,
    "max.in.flight.requests.per.connection": 3,
    "retries": 3,
}

SCHEMA_REGISTRY_CONFIG = {"url": "http://localhost:8081"}

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "analytics"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

POSTGRES_DSN = (
    f"postgresql://{POSTGRES_CONFIG['user']}:"
    f"{POSTGRES_CONFIG['password']}@"
    f"{POSTGRES_CONFIG['host']}:"
    f"{POSTGRES_CONFIG['port']}/"
    f"{POSTGRES_CONFIG['dbname']}"
)


class ClientAPICommands(StrEnum):
    """
    Команды терминала для пользовательского ввода.
    """

    SEARCH = "search"
    RECOMMENDATIONS = "recommendations"
    EXIT = "exit"


@dataclass(kw_only=True)
class BaseUserEvent:  # noqa: D101
    command: str
    query: str | None = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class UserRequestEvent(BaseUserEvent):  # noqa: D101
    user_id: str = field(default_factory=lambda: secrets.choice(USER_LIST))


@dataclass(kw_only=True)
class UserResponseEvent(BaseUserEvent):  # noqa: D101
    user_id: str
    results: list[dict[str, Any]] | None = None
