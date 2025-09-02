from faust import Record

from src.practice_3.base import ForbiddenAction


class Message(Record, serializer="json"):
    """
    Модель сообщения.
    """

    sender: str
    recipient: str
    text: str


class BlockUserEvent(Record, serializer="json"):
    """
    Модель блокировки пользователя.
    """

    user: str
    blocked: str


class CensoredMessage(Record, serializer="json"):
    """
    Модель зацензуренного сообщения.
    """

    sender: str
    recipient: str
    text: str


class ForbiddenWordEvent(Record, serializer="json"):
    """
    Модель запрещенного слова.
    """

    action: ForbiddenAction
    word: str
